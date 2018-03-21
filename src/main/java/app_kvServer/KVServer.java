package app_kvServer;

import com.google.gson.Gson;
import common.messages.KVAdminMessage;
import ecs.ECS;
import ecs.ECSHashRing;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import server.*;
import server.cache.KVCache;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CountDownLatch;


public class KVServer implements IKVServer, Runnable, Watcher {


    public static final Integer MAX_KEY = 20;
    public static final Integer MAX_VAL = 120 * 1024;
    public static final Integer BUFFER_SIZE = 1024;

    private static Logger logger = Logger.getRootLogger();

    private int port = -1;
    private int cacheSize;
    private CacheStrategy strategy;

    private boolean running;
    private ServerSocket serverSocket = null;
    private ServerSocket receiverSocket;

    private ServerStatus status;
    private String serverName;
    private boolean isDistributed = false;

    /* zookeeper info */
    private String zkHostName;
    private int zkPort;
    private ZooKeeper zk;
    private String zkPath;

    /* metadata */
    private ECSHashRing hashRing;
    private String hashRingString;

    /**
     * cache would be null if strategy is set to None
     */
    private KVCache cache;
    private KVPersistentStore store;

    /**
     * Forward put requests to server replicas
     */
    private KVServerForwarderManager forwarderManager;

    public String getHashRingString() {
        return hashRingString;
    }

    public String prompt() {
        return "(" + this.serverName + "): ";
    }

    /**
     * Start KV Server at given port
     *
     * @param port      giremainFileven port for storage server to operate
     * @param cacheSize specifies how many key-value pairs the server is allowed
     *                  to keep in-memory
     * @param strategy  specifies the cache replacement strategy in case the cache
     *                  is full and there is a GET- or PUT-request on a key that is
     *                  currently not contained in the cache. Options are "FIFO", "LRU",
     *                  and "LFU".
     */
    public KVServer(int port, int cacheSize, String strategy) {
        this(port, cacheSize, strategy, "iterateDataBase"); // Default db name
    }

    public KVServer(int port, int cacheSize, String strategy, String fileName) {
        this.status = ServerStatus.START;
        this.port = port;
        this.cacheSize = cacheSize;
        this.strategy = CacheStrategy.valueOf(strategy);
        if (this.strategy == CacheStrategy.None) {
            this.cache = null;
        } else {
            // Use reflection to dynamically initialize the cache based on strategy name
            try {
                Constructor<?> cons = Class.forName("server.cache.KV" + strategy + "Cache").getConstructor(Integer.class);
                this.cache = (KVCache) cons.newInstance(cacheSize);
            } catch (ClassNotFoundException |
                    NoSuchMethodException |
                    IllegalAccessException |
                    InstantiationException |
                    InvocationTargetException e) {
                logger.fatal("Component of KVServer is not found, please check the integrity of jar package");
                e.printStackTrace();
            }
        }
        this.store = new KVIterateStore(fileName);
    }

    public KVServer(Integer port, String name, String zkHostName, int zkPort) {
        this(name, zkHostName, zkPort);
        this.port = port;
    }

    public boolean isDistributed() {
        return isDistributed;
    }

    public KVServer(String name, String zkHostName, int zkPort) {
        this.isDistributed = true;
        this.zkHostName = zkHostName;
        this.serverName = name;
        this.zkPort = zkPort;
        this.status = ServerStatus.STOP;
        zkPath = ECS.ZK_SERVER_ROOT + "/" + name;
        String connectString = this.zkHostName + ":" + Integer.toString(this.zkPort);
        try {
            CountDownLatch sig = new CountDownLatch(0);
            zk = new ZooKeeper(connectString, ECS.ZK_TIMEOUT, event -> {
                if (event.getState().equals(Event.KeeperState.SyncConnected)) {
                    // connection fully established can proceed
                    sig.countDown();
                }
            });
            try {
                sig.await();
            } catch (InterruptedException e) {
                // Should never happen
                e.printStackTrace();
            }

        } catch (IOException e) {
            logger.debug(prompt() + "Unable to connect to zookeeper");
            e.printStackTrace();
        }

        try {
            // the node should be created before init the server
            if (zk.exists(zkPath, false) != null) {
                // retrieve cache info from zookeeper
                byte[] cacheData = zk.getData(zkPath, false, null);
                String cacheString = new String(cacheData);
                ServerMetaData json = new Gson().fromJson(cacheString, ServerMetaData.class);
                this.cacheSize = json.getCacheSize();
                this.strategy = CacheStrategy.valueOf(json.getCacheStrategy());
            } else {
                logger.error(prompt() + "Server node dose not exist " + zkPath);
            }


        } catch (InterruptedException | KeeperException e) {
            logger.error(prompt() + "Unable to retrieve cache info from " + zkPath);
            this.strategy = CacheStrategy.FIFO;
            this.cacheSize = 100;
            e.printStackTrace();
        }

        try {
            //remove the init message if have
            List<String> children = zk.getChildren(zkPath, false, null);
            if (!children.isEmpty()) {
                String messagePath = zkPath + "/" + children.get(0);
                byte[] data = zk.getData(messagePath, false, null);
                KVAdminMessage message = new Gson().fromJson(new String(data), KVAdminMessage.class);
                if (message.getOperationType().equals(KVAdminMessage.OperationType.INIT)) {
                    zk.delete(messagePath, zk.exists(messagePath, false).getVersion());
                    logger.info(prompt() + "Server initiated at constructor");
                }
            }
        } catch (InterruptedException | KeeperException e) {
            logger.error(prompt() + "Unable to get child nodes");
            e.printStackTrace();
        }

        try {
            // add an alive node for failure detection
            if (zk.exists(ECS.ZK_ACTIVE_ROOT, false) != null) {
                zk.create(ECS.ZK_ACTIVE_ROOT + "/" + this.serverName, "".getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                logger.debug(prompt() + "Alive node created");
            }

        } catch (KeeperException | InterruptedException e) {
            logger.error(prompt() + "Unable to create an ephemeral node");
            e.printStackTrace();
        }

        try {
            // setup hashRing info
            byte[] hashRingData = zk.getData(ECS.ZK_METADATA_ROOT, new Watcher() {
                // handle hashRing update
                public void process(WatchedEvent we) {
                    if (!running) {
                        return;
                    }
                    try {
                        byte[] hashRingData = zk.getData(ECS.ZK_METADATA_ROOT, this, null);
                        hashRingString = new String(hashRingData);
                        hashRing = new ECSHashRing(hashRingString);
                        logger.info(prompt() + "Hash Ring updated");
                        if (forwarderManager != null) {
                            forwarderManager.update(hashRing);
                        }
                    } catch (KeeperException | InterruptedException e) {
                        logger.error(prompt() + "Unable to update the metadata node");
                        e.printStackTrace();
                    } catch (IOException e) {
                        logger.error(prompt() + "Unable to update forward manager information");
                        e.printStackTrace();
                    }
                }
            }, null);
            logger.debug(prompt() + "Hash Ring found");
            hashRingString = new String(hashRingData);
            hashRing = new ECSHashRing(hashRingString);


        } catch (InterruptedException | KeeperException e) {
            logger.debug(prompt() + "Unable to get metadata info");
            e.printStackTrace();
        }

        if (this.strategy == CacheStrategy.None) {
            this.cache = null;
        } else {
            // Use reflection to dynamically initialize the cache based on strategy name
            try {
                Constructor<?> cons = Class.forName("server.cache.KV" + strategy
                        + "Cache").getConstructor(Integer.class);
                this.cache = (KVCache) cons.newInstance(cacheSize);
            } catch (ClassNotFoundException |
                    NoSuchMethodException |
                    IllegalAccessException |
                    InstantiationException |
                    InvocationTargetException e) {
                logger.fatal("Component of KVServer is not found, please check the integrity of jar package");
                e.printStackTrace();
            }
        }

        this.store = new KVIterateStore(name + "_iterateDataBase");

        try {
            // set watcher on childrens
            zk.getChildren(this.zkPath, this, null);
        } catch (InterruptedException | KeeperException e) {
            logger.debug(prompt() + "Unable to get set watcher on children");
            e.printStackTrace();
        }


    }

    @Override
    public void process(WatchedEvent event) {
        if (!running) {
            return;
        }
        List<String> children = null;
        try {
            children = zk.getChildren(zkPath, false, null);
            if (children.isEmpty()) {
                // re-register the watch
                zk.getChildren(zkPath, this, null);
                return;
            }

            String path = zkPath + "/" + children.get(0);

            // handling event, assume there is only one message named "message"
            byte[] data = zk.getData(path, false, null);
            KVAdminMessage message = new Gson().fromJson(new String(data), KVAdminMessage.class);
            switch (message.getOperationType()) {
                case INIT:
                    zk.delete(path, zk.exists(path, false).getVersion());
                    logger.info(prompt() + "Server initiated");
                    break;
                case SHUT_DOWN:
                    zk.delete(path, zk.exists(path, false).getVersion());
                    kill();
                    logger.info(prompt() + "Server shutdown");
                    break;
                case LOCK_WRITE:
                    break;
                case UNLOCK_WRITE:
                    break;
                case UPDATE:
                    break;
                case RECEIVE:
                    int receivePort = this.receiveData();

                    // set receive port in server node and update progress
                    byte[] rawMetaData = zk.getData(zkPath, false, null);
                    String metaDataString = new String(rawMetaData);
                    ServerMetaData metaData = new Gson().fromJson(metaDataString, ServerMetaData.class);
                    metaData.setReceivePort(receivePort);
                    metaData.setTransferProgress(0);

                    rawMetaData = new Gson().toJson(metaData).getBytes();
                    zk.setData(zkPath, rawMetaData,
                            zk.exists(zkPath, false).getVersion());

                    // delete the message node
                    zk.delete(path, zk.exists(path, false).getVersion());

                    logger.info(prompt() + "Waiting for data transfer on port " + receivePort + " " + zkPath);
                    break;

                case SEND:
                    String receiverName = message.getReceiverName();

                    // read receiver's node to get port
                    byte[] rawReceiverMetaData = zk.getData(ECS.ZK_SERVER_ROOT + "/" + receiverName,
                            false, null);
                    String receiverMetaDataString = new String(rawReceiverMetaData);
                    ServerMetaData receiverMetaData = new Gson().fromJson(receiverMetaDataString, ServerMetaData.class);
                    Integer receiverPort = receiverMetaData.getReceivePort();

                    // update its progress
                    byte[] rawSenderMetaData = zk.getData(zkPath, false, null);
                    String senderMetaDataString = new String(rawSenderMetaData);
                    ServerMetaData senderMetaData = new Gson().fromJson(senderMetaDataString, ServerMetaData.class);
                    senderMetaData.setTransferProgress(0);

                    rawSenderMetaData = new Gson().toJson(senderMetaData).getBytes();
                    zk.setData(zkPath, rawSenderMetaData,
                            zk.exists(zkPath, false).getVersion());

                    // delete the message node
                    zk.delete(path, zk.exists(path, false).getVersion());
                    logger.info(prompt() + "Server" + zkPath + "start sending....");

                    // send data
                    sendData(message.getHashRange(), message.getReceiverHost(), receiverPort, false);

                    break;

                case DELETE:
                    logger.debug(prompt() + "Receive delete message");
                    this.lockWrite();
                    ((KVIterateStore) this.store).deleteData(message.getHashRange());
                    this.unlockWrite();
                    this.clearCache();
                    logger.debug(prompt() + "Finish delete range");
                    zk.delete(path, zk.exists(path, false).getVersion());
                    break;

                case START:
                    this.start();
                    zk.delete(path, zk.exists(path, false).getVersion());
                    break;

                case STOP:
                    this.stop();
                    zk.delete(path, zk.exists(path, false).getVersion());
                    break;

            }

            // re-register the watch
            if (this.isRunning())
                zk.getChildren(zkPath, this, null);
        } catch (KeeperException | InterruptedException e) {
            logger.debug(prompt() + "Unable to process the watcher event");
            e.printStackTrace();
        }

    }


    public int receiveData() {
        try {
            receiverSocket = new ServerSocket(0);
            int port = receiverSocket.getLocalPort();
            new Thread(new KVServerReceiver(this, receiverSocket)).start();
            lockWrite();
            return port;
        } catch (IOException e) {
            logger.debug(prompt() + "Unable to open a receiver socket!");
            e.printStackTrace();
            return 0; // this exception should be handled by ecs
        }

    }

    public ServerStatus getServerStatus() {
        return status;
    }

    @Override
    public String getStorageName() {
        return this.store.getfileName();
    }

    @Override
    public ServerStatus getStatus() {
        return this.status;
    }

    @Override
    public int getPort() {
        return port;
    }

    public ECSHashRing getHashRing() {
        return hashRing;
    }

    public String getServerName() {
        return serverName;
    }

    @Override
    public String getHostname() {
        if (serverSocket == null) {
            return null;
        } else {
            return serverSocket.getInetAddress().getHostName();
        }
    }

    @Override
    public CacheStrategy getCacheStrategy() {
        return strategy;
    }

    @Override
    public int getCacheSize() {
        return cacheSize;
    }


    @Override
    public boolean inStorage(String key) {
        try {
            return store.inStorage(key);
        } catch (Exception e) {
            // when there is problem reading the file from disk
            // consider it as data not on disk
            logger.debug(prompt() + "Unable to access data file on disk!");
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean inCache(String key) {
        if (cache != null) {
            return cache.containsKey(key);
        } else {
            return false;
        }
    }

    @Override
    public synchronized String getKV(String key) throws Exception {
        if (cache != null) {
            if (this.inCache(key)) {
                return cache.get(key);
            } else {
                // Not in cache, read from disk and update cache
                String result = store.get(key);
                if (result != null) {
                    cache.put(key, result);
                }
                return result;
            }
        } else {
            return store.get(key);
        }
    }

    @Override
    public synchronized void putKV(String key, String value) throws Exception {
        // Update both cache and storage
        store.put(key, value);
        if (cache != null)
            cache.put(key, value);
    }

    @Override
    public void clearCache() {
        logger.info(prompt() + "Cache cleared");
        cache.clear();
    }

    @Override
    public void clearStorage() {
        store.clearStorage();
        cache.clear();
    }

    /**
     * Save the cache to disk first and then close server socket
     */
    @Override
    public void kill() {
        running = false;
        try {
            serverSocket.close();
            forwarderManager.clear();
            zk.close();
        } catch (IOException e) {
            logger.debug(prompt() + "Unable to close socket on port: " + port, e);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        kill();
        clearCache();
    }

    @Override
    public void start() {
        logger.info(prompt() + "Server started");
        this.status = ServerStatus.START;
    }

    @Override
    public void stop() {
        logger.info(prompt() + "Server stoped");
        this.status = ServerStatus.STOP;
    }

    @Override
    public void lockWrite() {
        logger.info(prompt() + "Server status change to WRITE_LOCK");
        this.status = ServerStatus.LOCK;
    }

    @Override
    public void unlockWrite() {
        logger.info(prompt() + "Server Unlock Write");
        this.status = ServerStatus.START;
    }

    @Override
    public boolean moveData(String[] hashRange, String targetName) throws Exception {
        // Don't want to get ip and port from target name
        return false;
    }


    public void updateTransferProgress(int transferProgress) {
        try {
            byte[] rawSenderMetaData = zk.getData(zkPath, false, null);
            String senderMetaDataString = new String(rawSenderMetaData);
            ServerMetaData senderMetaData = new Gson().fromJson(senderMetaDataString, ServerMetaData.class);
            senderMetaData.setTransferProgress(transferProgress);

            rawSenderMetaData = new Gson().toJson(senderMetaData).getBytes();
            Stat stat = zk.setData(zkPath, rawSenderMetaData,
                    zk.exists(zkPath, false).getVersion());
            logger.debug(prompt() + "Update TransferProgress: " + transferProgress);

        } catch (InterruptedException | KeeperException e) {
            logger.debug(prompt() + "Unable to update progress");
            e.printStackTrace();
        }

    }


    public boolean sendData(String[] hashRange, String targetHost, int targetPort) {
        return sendData(hashRange, targetHost, targetPort, true);
    }

    public boolean sendData(String[] hashRange, String targetHost, int targetPort, boolean shouldDelete) {
        try {
            this.lockWrite();

            ((KVIterateStore) this.store).preMoveData(hashRange);

            String moveFileName = this.store.getfileName() + KVIterateStore.MOVE_SUFFIX;
            File moveFile = new File(moveFileName);
            long fileLength = moveFile.length();

            logger.debug(prompt() + "fileLength: " + fileLength);
            byte[] buffer = new byte[BUFFER_SIZE];
            Socket clientSocket = new Socket(targetHost, targetPort);
            BufferedOutputStream out = new BufferedOutputStream(clientSocket.getOutputStream());
            BufferedInputStream in = new BufferedInputStream(new FileInputStream(moveFile));

            long totalLength = 0;
            int len;
            int progress = 0;
            while ((len = in.read(buffer)) > 0) {
                // update percentage
                progress = (int) ((float) totalLength / (float) fileLength * 100.0);
                logger.debug(prompt() + "totalLength: " + totalLength);
                // write into zookeeper
                updateTransferProgress(progress);

                out.write(buffer, 0, len);
                totalLength += len;
            }

            updateTransferProgress(100);
            in.close();
            out.flush();
            out.close();
            clientSocket.close();

            ((KVIterateStore) this.store).afterMoveData(shouldDelete);

            if (shouldDelete) {
                this.clearCache();
            }

            this.unlockWrite();

            logger.info(prompt() + "Finish transferring data");

            return true;
        } catch (IOException e) {
            logger.debug(prompt() + "Unable to connect receiver");
            e.printStackTrace();
            this.unlockWrite();
            return false;
        }

    }

    @Override
    public void run() {
        running = initializeServer();
        if (serverSocket != null) {
            while (isRunning()) {
                try {
                    Socket client = serverSocket.accept();
                    KVServerConnection conn = new KVServerConnection(this, client);
                    new Thread(conn).start();

                    logger.info(prompt() + "Connected to "
                            + client.getInetAddress().getHostName()
                            + " on port " + client.getPort());
                } catch (IOException e) {
                    logger.debug(prompt() + "Unable to establish connection with client.\n", e);
                }
            }
        }
        logger.info(prompt() + "Server Shutdown");
    }

    public boolean isRunning() {
        return running;
    }

    public KVServerForwarderManager getForwarderManager() {
        return forwarderManager;
    }

    private boolean initializeServer() {
        assert this.port != -1;
        try {
            serverSocket = new ServerSocket(port);
            logger.info(prompt() + "Server listening on port: "
                    + serverSocket.getLocalPort());
            this.port = serverSocket.getLocalPort();
            this.forwarderManager = new KVServerForwarderManager(this.getServerName(), getHostname(), this.port);
            return true;
        } catch (IOException e) {
            logger.error(prompt() + "Error! Cannot open server socket:");
            if (e instanceof BindException) {
                logger.error(prompt() + "Port " + port + " is already bound!");
            }
            return false;
        }
    }

    /**
     * Main entry point for the KVServer application.
     *
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
        try {
            new LogSetup("logs/server.log", Level.ALL);
            if (args.length < 3 || args.length > 4) {
                System.err.println("Error! Invalid number of arguments!");
                System.err.println("Usage: Server <port> <cache size> <strategy>!");
                System.err.println("Usage: Server <port> <serverName> <kvHost> <kvPort>!");
            } else if (args.length == 3) {
                new Thread(new KVServer(
                        Integer.parseInt(args[0]),
                        Integer.parseInt(args[1]),
                        args[2]
                )).start();
            } else {
                KVServer server = new KVServer(Integer.parseInt(args[0]), args[1], args[2], Integer.parseInt(args[3]));
                new Thread(server).start();
            }
        } catch (NumberFormatException nfe) {
            System.err.println("Error! Invalid <port> or Invalid <cache size>! Not a number!");
            System.exit(1);
        } catch (IllegalArgumentException iae) {
            System.err.println("Error! Invalid <strategy>! Must be one of [None LRU LFU FIFO]!");
            System.exit(1);
        } catch (IOException ioe) {
            System.err.println("Error! Unable to initialize logger!");
            ioe.printStackTrace();
            System.exit(1);
        }
    }

}



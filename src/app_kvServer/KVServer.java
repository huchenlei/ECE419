package app_kvServer;

import com.google.gson.Gson;
import common.messages.KVAdminMessage;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import server.*;
import server.cache.KVCache;
import server.cache.KVFIFOCache;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;


public class KVServer implements IKVServer, Runnable, Watcher {


    public enum ServerStatus {
        START,       /* server works correctly */
        STOP,        /* no client requests are processed */
        LOCK         /* server is currently blocked for write requests */
    }

    public static final Integer MAX_KEY = 20;
    public static final Integer MAX_VAL = 120 * 1024;
    public static final Integer BUFFER_SIZE = 1024;

    private static Logger logger = Logger.getRootLogger();

    private int port;
    private int cacheSize;
    private CacheStrategy strategy;

    private boolean running;
    private ServerSocket serverSocket;
    private ServerSocket receiverSocket;

    private ServerStatus status;
    private String serverName;

    /* zookeeper info */
    private static final int ZK_TIMEOUT = 2000;
    private static final String ZK_SERVER_ROOT = "/kv_servers";
    private String zkHostName;
    private int zkPort;
    private ZooKeeper zk;
    private String zkPath;

    /**
     * cache would be null if strategy is set to None
     */
    private KVCache cache;
    private KVPersistentStore store;

    /**
     * Start KV Server at given port
     *
     * @param port      given port for storage server to operate
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

    public KVServer(String name, String zkHostName, int zkPort) {
        this.zkHostName = zkHostName;
        this.serverName = name;
        this.zkPort = zkPort;
        zkPath = ZK_SERVER_ROOT + "/" + name;
        String connectString = this.zkHostName + ":" + Integer.toString(this.zkPort);
        try {
            CountDownLatch sig = new CountDownLatch(0);
            zk = new ZooKeeper(connectString, ZK_TIMEOUT, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getState().equals(Event.KeeperState.SyncConnected)) {
                        // connection fully established can proceed
                        sig.countDown();
                    }
                }
            });
            try {
                sig.await();
            } catch (InterruptedException e) {
                // Should never happen
                e.printStackTrace();
            }
            // the node should be created before init the server
            Stat stat = zk.exists(zkPath, false);

            // retrieve cache info from zookeeper
            byte[] cacheData = zk.getData(zkPath, false, null);
            String cacheString = new String(cacheData);
            ServerMetaData json = new Gson().fromJson(cacheString, ServerMetaData.class);
            this.cacheSize = json.getCacheSize();
            this.strategy = CacheStrategy.valueOf(json.getCacheStrategy());

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
            this.store = new KVIterateStore(name + "_iterateDataBase");

            // set watcher on childrens
            zk.getChildren(this.zkPath, this, null);

        } catch (IOException | InterruptedException | KeeperException e) {
            logger.error("Unable to connect to zookeeper");
            e.printStackTrace();
        }

    }

    @Override
    public void process(WatchedEvent event) {
        List<String> children = null;
        try {
            children = zk.getChildren(zkPath, false, null);
            if (children.isEmpty() || !("message").equals(children.get(0))) {
                // re-register the watch
                zk.getChildren(zkPath, this, null);
                return;
            }

            String path = zkPath + "message";

            // handling event, assume there is only one message named "message"
            byte[] data = zk.getData(zkPath + "message", false, null);
            KVAdminMessage message = new Gson().fromJson(new String(data), KVAdminMessage.class);
            switch (message.getOperationType()) {
                case SHUT_DOWN:
                    break;
                case LOCK_WRITE:
                    break;
                case UNLOCK_WRITE:
                    break;
                case UPDATE:
                    break;
                case RECEIVE:
                    int receivePort = this.receiveData();
                    // simply write a integer in it
                    zk.setData(path, Integer.toString(receivePort).getBytes(),
                            zk.exists(path, false).getVersion());
                    logger.info("Waiting for data transfer on port " + receivePort);
                    break;

                case SEND:
                    // TODO: think about it
                    sendData(message.getHashRange(), message.getReceiverHost(), message.getReceiverPort());
                    break;

                case START:
                    this.start();
                    zk.delete(path, zk.exists(path, false).getVersion());
                    logger.info("Server started.");
                    break;

                case STOP:
                    this.stop();
                    zk.delete(path, zk.exists(path, false).getVersion());
                    logger.info("Server stopped.");
                    break;

            }

            // re-register the watch
            zk.getChildren(zkPath, this, null);
        } catch (KeeperException | InterruptedException e) {
            logger.error("Unable to process the watcher event");
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
            logger.error("Unable to open a receiver socket!");
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
    public int getPort() {
        return port;
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
            logger.error("Unable to access data file on disk!");
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
        } catch (IOException e) {
            logger.error("Unable to close socket on port: " + port, e);
        }
        // TODO Store everything in cache but not in storage
        // TODO Currently there shall not be any concern on this issue
        // TODO since we do store to disk prior to store to cache
    }

    @Override
    public void close() {
        kill();
        clearCache();
    }

    @Override
    public void start() {
        this.status = ServerStatus.START;
    }

    @Override
    public void stop() {
        this.status = ServerStatus.STOP;
    }

    @Override
    public void lockWrite() {
        logger.info("Server status change to WRITE_LOCK");
        this.status = ServerStatus.LOCK;
    }

    @Override
    public void unlockWrite() {
        logger.info("Server Unlock Write");
        this.status = ServerStatus.START;
    }

    @Override
    public boolean moveData(String[] hashRange, String targetName) throws Exception {
        // Don't want to get ip and host from target name
        return false;
    }

    public boolean sendData(String[] hashRange, String targetHost, int targetPort) {
        try {
            this.lockWrite();

            // TODO: move in range kv pairs into a new file


            byte[] buffer = new byte[BUFFER_SIZE];
            Socket clientSocket = new Socket(targetHost, targetPort);
            BufferedOutputStream out = new BufferedOutputStream(clientSocket.getOutputStream());
            BufferedInputStream in = new BufferedInputStream(new FileInputStream(this.store.getfileName()));

            int len = 0;
            while ((len = in.read(buffer)) > 0) {
                out.write(buffer, 0, len);
            }
            in.close();
            out.flush();
            out.close();
            clientSocket.close();
            this.unlockWrite();

            logger.info("Finish transferring data");

            return true;
        } catch (IOException e) {
            logger.error("Unable to connect receiver");
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

                    logger.info("Connected to "
                            + client.getInetAddress().getHostName()
                            + " on port " + client.getPort());
                } catch (IOException e) {
                    logger.error("Unable to establish connection with client.\n", e);
                }
            }
        }
    }

    public boolean isRunning() {
        return running;
    }

    private boolean initializeServer() {
        logger.info("Initialize server ...");
        try {
            serverSocket = new ServerSocket(port);
            logger.info("Server listening on port: "
                    + serverSocket.getLocalPort());
            this.port = serverSocket.getLocalPort();
            return true;
        } catch (IOException e) {
            logger.error("Error! Cannot open server socket:");
            if (e instanceof BindException) {
                logger.error("Port " + port + " is already bound!");
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
                KVServer server = new KVServer(args[1], args[2], Integer.parseInt(args[3]));
                server.port = Integer.parseInt(args[0]);
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



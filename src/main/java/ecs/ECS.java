package ecs;

import app_kvECS.IECSClient;
import app_kvServer.KVServer;
import com.google.gson.Gson;
import common.NetworkUtils;
import common.messages.KVAdminMessage;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Service;
import server.ServerMetaData;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

/**
 * This class handles core functionality of external configuration service
 */

@Service
@PropertySource("classpath:/app.properties")
public class ECS implements IECSClient {
    private static final String SERVER_JAR = "KVServer.jar";
    // Assumes that the jar file is located at the same dir on the remote server
    private static final String JAR_PATH = new File(System.getProperty("user.dir"), SERVER_JAR).toString();
    // Assumes that ZooKeeper runs on localhost default port(2181)
    public static final String LOCAL_HOST = NetworkUtils.getCurrentHost();
    public static final String ZK_HOST = LOCAL_HOST;
    public static final String ZK_PORT = "2181";
    public static final String ZK_CONN = ZK_HOST + ":" + ZK_PORT;

    // ZooKeeper connection timeout in millisecond
    public static final int ZK_TIMEOUT = 2000;

    public static final String ZK_SERVER_ROOT = "/kv_servers";
    public static final String ZK_ACTIVE_ROOT = "/active";
    public static final String ZK_METADATA_ROOT = "/metadata";

    public boolean locally = false;
    private static Logger logger = Logger.getRootLogger();

    private String restoreFileName = "ecs_restore_list";
    private File restoreFile = new File(this.restoreFileName);
    private Map<String, IECSNode> restoreList = new HashMap<>();

    /**
     * Roster of all <b>idle<b/> storage service nodes
     */
    private Queue<IECSNode> nodePool = new ConcurrentLinkedQueue<>();

    /**
     * Roster of all service that are initialized(assigned cacheStrategy and cacheSize)
     * Used for interface provided by professor
     *
     * @Deprecated
     */
    private Map<String, IECSNode> nodeTable = new HashMap<>();

    /**
     * In charge of all nodes no matter the state
     * Used for web monitor program
     */
    private Map<String, ECSNode> generalNodeTable = new HashMap<>();

    /**
     * HashRing object responsible to update the hash range of each ECSNode
     */
    private ECSHashRing hashRing;

    /**
     * Handling data transfer
     */
    private ECSDataDistributionManager manager;

    /**
     * ZooKeeper instance used to communicate with zk server
     */
    private ZooKeeper zk;


    public class ECSConfigFormatException extends RuntimeException {
        public ECSConfigFormatException(String msg) {
            super(msg);
        }
    }

    public static class ECSException extends Exception {
        public ECSException(String msg) {
            super(msg);
        }
    }

    public ECS(@Value("${ecsConfigFile}") String configFileName) throws IOException {
        this.hashRing = new ECSHashRing();
        this.manager = new ECSDataDistributionManager(this.hashRing);

        BufferedReader configReader = new BufferedReader(new FileReader(new File(configFileName)));

        String currentLine;
        while ((currentLine = configReader.readLine()) != null) {
            String[] tokens = currentLine.split(" ");
            if (tokens.length != 3) {
                throw new ECSConfigFormatException("invalid number of arguments! should be 3 but got " +
                        tokens.length + ".");
            }
            String name = tokens[0];
            String ip = tokens[1];
            // Convert localhost to actual host
            if (ip.equals("localhost") || ip.equals("127.0.0.1"))
                ip = LOCAL_HOST;

            Integer port = Integer.parseInt(tokens[2]);
            try {
                createNode(name, ip, port);
            } catch (ECSException e) {
                logger.warn(e.getMessage());
            }
        }

        CountDownLatch sig = new CountDownLatch(0);
        zk = new ZooKeeper(ZK_CONN, ZK_TIMEOUT, event -> {
            if (event.getState().equals(Watcher.Event.KeeperState.SyncConnected)) {
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

        updateMetadata();

        logger.info("ECS started with ZooKeeper " + ZK_CONN);
    }

    /* ---------- Following are methods exposing ecs details to web console ---------- */
    public List<RawECSNode> getAllNodes() {
        return new ArrayList<>(generalNodeTable.values());
    }

    public RawECSNode getNodeByName(String name) {
        return generalNodeTable.get(name);
    }

    public void createNode(String name, String host, Integer port) throws ECSException {
        if (generalNodeTable.containsKey(name)) {
            throw new ECSException(name + " already exists. Server name must be unique");
        }
        ECSNode newNode = new ECSNode(name, host, port);
        nodePool.add(newNode);
        generalNodeTable.put(name, newNode);
        logger.info(newNode + " added to node pool");
    }

    @Override
    public boolean start() throws Exception {
        // restore servers shut down last time
        this.restoreServer();

        List<ECSNode> toStart = new ArrayList<>();
        List<ECSNode> toRestore = new ArrayList<>();
        List<ECSNode> toClear = new ArrayList<>();

        for (Map.Entry<String, IECSNode> entry : nodeTable.entrySet()) {
            ECSNode n = (ECSNode) entry.getValue();
            if (n.getStatus().equals(ECSNode.ServerStatus.STOP)) {
                toStart.add(n);
                // restored server will be handled separately
                if (this.restoreList.get(n.getNodeName()) == null) {
                    toClear.add(n);
                } else {
                    toRestore.add(n);
                }
            }
        }

        boolean ret = true;

        // restore server first
        for (ECSNode n : toRestore) {
            // add the nodes without transferring data
            logger.info("Add ndoe to hashring " + n.getNodeName());
            manager.addNode(n);
        }

        // clear storage for non-restored server;
        ECSMulticaster multicaster = new ECSMulticaster(zk, toClear);
        ret &= multicaster.send(new KVAdminMessage(KVAdminMessage.OperationType.CLEAR));
        for (ECSNode n : toClear) {
            List<ECSDataTransferIssuer> transfers = manager.addNode(n);
            for (ECSDataTransferIssuer transfer : transfers) {
                logger.info(transfer);
                ret &= transfer.start(zk);
            }
        }
        multicaster = new ECSMulticaster(zk, toStart);
        ret = multicaster.send(new KVAdminMessage(KVAdminMessage.OperationType.START));

        for (ECSNode n : toStart) {
            if (multicaster.getErrors().keySet().contains(n)) {
                hashRing.removeNode(n);
            } else {
                n.setStatus(ECSNode.ServerStatus.ACTIVE);
            }
        }

        clearRestoreList();
        updateMetadata();
        return ret;
    }

    public void clearRestoreList() throws IOException {
        // clear restore list
        this.restoreList.clear();
        if (this.restoreFile != null) {
            // clear the file
            new PrintWriter(this.restoreFile);
            logger.info("Restore file cleared");
        }
    }

    @Override
    public boolean stop() throws Exception {
        List<ECSNode> toStop = new ArrayList<>();
        for (Map.Entry<String, IECSNode> entry : nodeTable.entrySet()) {
            ECSNode n = (ECSNode) entry.getValue();
            if (n.getStatus().equals(ECSNode.ServerStatus.ACTIVE)) {
                toStop.add(n);
            }
        }

        ECSMulticaster multicaster = new ECSMulticaster(zk, toStop);
        boolean ret = multicaster.send(new KVAdminMessage(KVAdminMessage.OperationType.STOP));

        for (ECSNode n : toStop) {
            if (!multicaster.getErrors().keySet().contains(n)) {
                hashRing.removeNode(n);
            }
        }

        // save the restore list
        this.saveStoreList(toStop);

        toStop.forEach(n -> n.setStatus(ECSNode.ServerStatus.STOP));

        updateMetadata();
        return ret;
    }

    @Override
    public boolean shutdown() throws Exception {

        boolean ret = true;
        // get the restore list
        List<ECSNode> toRestore = new ArrayList<>();
        for (IECSNode n : this.nodeTable.values()) {
            ECSNode node = (ECSNode) n;
            if (node.status.equals(ECSNode.ServerStatus.ACTIVE)) {
                toRestore.add(node);
            }
        }

        this.saveStoreList(toRestore);
        hashRing.removeAll();
        nodeTable.values()
                .forEach(n -> ((ECSNode) n).setStatus(ECSNode.ServerStatus.OFFLINE));
        ret = updateMetadata();

        ECSMulticaster multicaster = new ECSMulticaster(zk, nodeTable.values()
                .stream().map((n) -> (ECSNode) n).collect(Collectors.toList()));
        ret = multicaster.send(new KVAdminMessage(KVAdminMessage.OperationType.SHUT_DOWN));

        nodePool.addAll(nodeTable.values());
        nodeTable.clear();
        return ret;
    }

    public void saveStoreList(Collection<ECSNode> nodeList) {
        try {
            if (this.restoreFile == null) {
                this.restoreFile = new File(this.restoreFileName);
            }
            if (!this.restoreFile.exists()) {
                logger.info("New restore file created");
                this.restoreFile.createNewFile();
            }
            BufferedWriter output = new BufferedWriter(new FileWriter(restoreFile, true));

            for (ECSNode n : nodeList) {
                String line = String.join(" ", n.getNodeName(), n.cacheStrategy,
                        n.cacheSize.toString());
                output.append(line + "\r\n");

            }
            output.close();

            logger.info("restore file updated");
        } catch (IOException e) {
            logger.error("Unable to save the restore List" + e.getMessage());
        }


    }

    static String getNodePath(IECSNode node) {
        return ZK_SERVER_ROOT + "/" + node.getNodeName();
    }

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        Collection<IECSNode> nodes = addNodes(1, cacheStrategy, cacheSize);
        if (nodes == null) return null;
        assert nodes.size() <= 1;
        return nodes.size() == 1 ? (IECSNode) nodes.toArray()[0] : null;
    }

    public void addNodes(Collection<IECSNode> nodes, String cacheStrategy, int cacheSize) {
        setupNodes(nodes, cacheStrategy, cacheSize);
        invokeNodes(nodes);
    }

    public void addNodesLocally(Collection<IECSNode> nodes, String cacheStrategy, int cacheSize) {
        setupNodes(nodes, cacheStrategy, cacheSize);
        for (IECSNode node : nodes) {
            KVServer server = new KVServer(node.getNodePort(), node.getNodeName(),
                    ECS.ZK_HOST, Integer.parseInt(ECS.ZK_PORT));
            new Thread(server).start();
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        try {
            awaitNodes(nodes.size(), ZK_TIMEOUT);
        } catch (Exception e) {
            logger.error(e);
        }
    }

    public Collection<IECSNode> invokeNodes(Collection<IECSNode> nodes) {
        for (IECSNode n : nodes) {
            assert n != null;
            // Issue the ssh call to start the process remotely
            String javaCmd = String.join(" ",
                    "java -jar",
                    JAR_PATH,
                    String.valueOf(n.getNodePort()),
                    n.getNodeName(),
                    ZK_HOST,
                    ZK_PORT);
            String sshCmd = "ssh -o StrictHostKeyChecking=no -n " + n.getNodeHost() + " nohup " + javaCmd +
                    " > server.log &";
            // Redirect output to files so that ssh channel will not wait for further output
            try {
                logger.info("Executing command: " + sshCmd);
                Process p = Runtime.getRuntime().exec(sshCmd);
                Thread.sleep(100);
                // p.waitFor();
                // assert !p.isAlive();
                // assert p.exitValue() == 0;
            } catch (IOException e) {
                logger.error("Unable to launch server with ssh (" + n + ")", e);
                e.printStackTrace();
                nodes.remove(n); // Connection failed, remove instance from result collection
            } catch (InterruptedException e) {
                logger.error("Receive an interrupt", e);
                e.printStackTrace();
                nodes.remove(n);
            }
        }

        boolean ack;
        try {
            ack = awaitNodes(nodes.size(), ZK_TIMEOUT);
        } catch (Exception e) {
            logger.error(e);
            return null;
        }
        if (ack)
            return nodes;
        else
            return null;
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        Collection<IECSNode> nodes = setupNodes(count, cacheStrategy, cacheSize);
        if (nodes == null) return null;

        return invokeNodes(nodes);
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        if (count > nodePool.size()) return null;

        List<IECSNode> nodeList = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            ECSNode n = (ECSNode) nodePool.poll();
            nodeList.add(n);
        }

        return setupNodes(nodeList, cacheStrategy, cacheSize);

    }

    public Collection<IECSNode> setupNodes(Collection<IECSNode> nodeList, String cacheStrategy, int cacheSize) {

        byte[] metadata = new Gson().toJson(new ServerMetaData(cacheStrategy, cacheSize)).getBytes();
        // create corresponding Z-nodes on zookeeper server
        try {


            Stat existActive = zk.exists(ZK_ACTIVE_ROOT, false);
            if (existActive == null) {
               	zk.create(ZK_ACTIVE_ROOT, "".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }


            if (zk.exists(ZK_SERVER_ROOT, false) == null) {
                zk.create(ZK_SERVER_ROOT, "".getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }

            for (IECSNode n : nodeList) {
                ((ECSNode) n).cacheStrategy = cacheStrategy;
                ((ECSNode) n).cacheSize = cacheSize;

                Stat exists = zk.exists(getNodePath(n), false);
                if (exists == null) {
                    zk.create(getNodePath(n), metadata,
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } else {
                    zk.setData(getNodePath(n), metadata, exists.getVersion());
                    // Delete all children (msg z-nodes)
                    List<String> children = zk.getChildren(getNodePath(n), false);
                    for (String zn : children) {
                        String msgPath = getNodePath(n) + "/" + zn;
                        Stat ex = zk.exists(msgPath, false);
                        zk.delete(msgPath, ex.getVersion());
                    }
                }
            }
        } catch (InterruptedException | KeeperException e) {
            logger.error("Issue encountered with ZooKeeper server");
            e.printStackTrace();
            return null;
        }

        for (IECSNode n : nodeList) {
            ((ECSNode) n).setStatus(ECSNode.ServerStatus.INACTIVE);
            nodeTable.put(n.getNodeName(), n);
        }

        return nodeList;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        List<ECSNode> toWait = nodeTable.values().stream()
                .map((n) -> (ECSNode) n)
                .filter((n) -> n.getStatus().equals(ECSNode.ServerStatus.INACTIVE))
                .collect(Collectors.toList());

        ECSMulticaster multicaster = new ECSMulticaster(zk, toWait);
        boolean ret = multicaster.send(new KVAdminMessage(KVAdminMessage.OperationType.INIT));

        for (ECSNode node : toWait) {
            node.setStatus(ECSNode.ServerStatus.STOP);
            Stat exists = zk.exists(ZK_ACTIVE_ROOT + "/" + node.getNodeName(),
                    new ECSFailureDetector(this, node.getNodeName()));
            assert exists != null;
        }

        return ret;
    }

    public boolean handleNodeShutDown(String name) {
        boolean ret = true;
        for (ECSDataTransferIssuer transferIssuer :
                manager.removeNode(generalNodeTable.get(name))) {
            try {
                ret &= transferIssuer.start(zk);
            } catch (InterruptedException e) {
                ret = false;
                e.printStackTrace();
            }
        }
        nodeTable.remove(name);
        nodePool.add(generalNodeTable.get(name));
        ret = updateMetadata();
        return ret;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        List<ECSNode> toRemove = nodeTable.values().stream()
                .map((n) -> (ECSNode) n)
                .filter(n -> nodeNames.contains(n.getNodeName()))
                .collect(Collectors.toList());

        logger.info("Remove following node from node table");
        toRemove.forEach(logger::info);
        logger.info("\n");

        logger.info("Current hashRing state:");
        logger.info(hashRing);

        boolean ret = true;
        try {
            for (ECSNode n : toRemove.stream()
                    .filter(n -> n.getStatus().equals(ECSNode.ServerStatus.ACTIVE))
                    .collect(Collectors.toList())) {
                List<ECSDataTransferIssuer> transfers = manager.removeNode(n);
                for (ECSDataTransferIssuer transfer : transfers) {
                    logger.info(transfer);
                    ret &= transfer.start(zk);
                }
            }

            for (ECSNode n : toRemove) {
                n.setStatus(ECSNode.ServerStatus.OFFLINE);
                nodeTable.remove(n.getNodeName());
            }

            nodePool.addAll(toRemove);
            ECSMulticaster multicaster = new ECSMulticaster(zk, toRemove);
            ret = multicaster.send(new KVAdminMessage(KVAdminMessage.OperationType.SHUT_DOWN));
        } catch (InterruptedException e) {
            e.printStackTrace();
            ret = false;
        }
        if (ret)
            ret = updateMetadata();

        return ret;
    }

    @Override
    public Map<String, IECSNode> getNodes() {
        return nodeTable;
    }

    @Override
    public IECSNode getNodeByKey(String key) {
        return hashRing.getNodeByKey(key);
    }

    /**
     * Convert currently active node to an json array
     *
     * @return Json Array
     */
    public String getHashRingJson() {
        List<RawECSNode> activeNodes = nodeTable.values().stream()
                .map(n -> (ECSNode) n)
                .filter(n -> n.getStatus().equals(ECSNode.ServerStatus.ACTIVE))
                .map(RawECSNode::new)
                .collect(Collectors.toList());

        return new Gson().toJson(activeNodes);
    }

    /**
     * Restore last time shutdowned server.
     */
    public void restoreServer() {
        if (this.restoreFile == null) {
            this.restoreFile = new File(this.restoreFileName);
        }
        try {
            // create one if file does not exist
            if (!this.restoreFile.exists()) {
                this.restoreFile.createNewFile();
                logger.info("New restore file created, no shut downed server last time.");
                return;

            } else {
                logger.debug("Restore list found.");

                // parsing the restore file
                BufferedReader restoreReader = new BufferedReader(new FileReader(this.restoreFile));
                String currentLine;
                List<String> restoreLineList = new ArrayList<String>();
                while ((currentLine = restoreReader.readLine()) != null) {
                    restoreLineList.add(currentLine);
                }
                if (restoreLineList.isEmpty()) {
                    logger.info("restore list is empty, no server to restore");
                } else {
                    for (String restoreLine : restoreLineList) {
                        String[] tokens = restoreLine.split(" ");
                        if (tokens.length != 3) {
                            logger.error("Invalid restore line token format");
                            throw new IOException();
                        }
                        // get restored info
                        String restoreName = tokens[0];
                        String cacheStrategy = tokens[1];
                        Integer cacheSize = Integer.parseInt(tokens[2]);

                        ECSNode restoreNode = generalNodeTable.get(restoreName);
                        // should not be null
                        assert restoreNode != null;
                        this.restoreList.put(restoreNode.getNodeName(), restoreNode);

                        // if node is offline
                        // restore cacheStrategy and cache size separately
                        if (restoreNode.getStatus().equals(ECSNode.ServerStatus.OFFLINE)) {
                            // create a single element list
                            logger.info("Restoring: " + restoreNode.getNodeName());
                            List<IECSNode> singleList = new ArrayList<>(Arrays.asList(restoreNode));
                            if (this.locally) {
                                this.addNodesLocally(singleList, cacheStrategy, cacheSize);
                            } else {
                                this.addNodes(singleList, cacheStrategy, cacheSize);
                            }
                        }
                    }

                }

            }
        } catch (IOException e) {
            logger.error("Error occurred when restore servers " + e.getMessage());
        }
    }

    /**
     * Push the metadata(hash ring content) to ZooKeeper z-node
     */
    private boolean updateMetadata() {
        try {
            Stat exists = zk.exists(ZK_METADATA_ROOT, false);
            if (exists == null) {
                zk.create(ZK_METADATA_ROOT, getHashRingJson().getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } else {
                zk.setData(ZK_METADATA_ROOT, getHashRingJson().getBytes(),
                        exists.getVersion());
            }
        } catch (InterruptedException e) {
            logger.error("Interrupted");
            return false;
        } catch (KeeperException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
            return false;
        }
        return true;
    }
}

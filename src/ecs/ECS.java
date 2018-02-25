package ecs;

import app_kvECS.IECSClient;
import com.google.gson.Gson;
import common.messages.KVAdminMessage;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import server.ServerMetaData;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * This class handles core functionality of external configuration service
 */
public class ECS implements IECSClient {
    private static final String SERVER_JAR = "KVServer.jar";
    // Assumes that the jar file is located at the same dir on the remote server
    private static final String JAR_PATH = new File(System.getProperty("user.dir"), SERVER_JAR).toString();
    // Assumes that ZooKeeper runs on localhost default port(2181)
    public static final String ZK_HOST = "127.0.0.1";
    public static final String ZK_PORT = "2181";
    private static final String ZK_CONN = ZK_HOST + ":" + ZK_PORT;

    // ZooKeeper connection timeout in millisecond
    public static final int ZK_TIMEOUT = 5000;

    public static final String ZK_SERVER_ROOT = "/kv_servers";
    public static final String ZK_METADATA_ROOT = "/metadata";

    private static Logger logger = Logger.getRootLogger();

    /**
     * Roster of all <b>idle<b/> storage service nodes
     */
    private Queue<IECSNode> nodePool = new ConcurrentLinkedQueue<>();

    /**
     * Roster of all service that are initialized(assigned cacheStrategy and cacheSize)
     */
    private Map<String, IECSNode> nodeTable = new HashMap<>();

    /**
     * HashRing object responsible to update the hash range of each ECSNode
     */
    private ECSHashRing hashRing = new ECSHashRing();

    /**
     * ZooKeeper instance used to communicate with zk server
     */
    private ZooKeeper zk;

    public class ECSConfigFormatException extends RuntimeException {
        public ECSConfigFormatException(String msg) {
            super(msg);
        }
    }

    public ECS(String configFileName) throws IOException {
        BufferedReader configReader = new BufferedReader(new FileReader(new File(configFileName)));

        String currentLine;
        Set<String> namePool = new HashSet<>();
        while ((currentLine = configReader.readLine()) != null) {
            String[] tokens = currentLine.split(" ");
            if (tokens.length != 3) {
                throw new ECSConfigFormatException("invalid number of arguments! should be 3 but got " +
                        tokens.length + ".");
            }
            String name = tokens[0];
            String ip = tokens[1];
            Integer port = Integer.parseInt(tokens[2]);
            if (namePool.contains(name)) {
                logger.warn(name + " already exists. Server name must be unique, please check for duplications");
            } else {
                ECSNode newNode = new ECSNode(name, ip, port);
                nodePool.add(newNode);
                namePool.add(name);
                logger.info(newNode + " added to node pool");
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
    }

    @Override
    public boolean start() throws Exception {
        List<ECSNode> toStart = new ArrayList<>();
        for (Map.Entry<String, IECSNode> entry : nodeTable.entrySet()) {
            ECSNode n = (ECSNode) entry.getValue();
            if (n.getStatus().equals(ECSNode.ServerStatus.STOP)) {
                toStart.add(n);
            }
        }
        for (ECSNode n : toStart) {
            hashRing.addNode(n);
        }

        rearrangeDataStorage(toStart);

        ECSMulticaster multicaster = new ECSMulticaster(zk, toStart);
        boolean ret = multicaster.send(new KVAdminMessage(KVAdminMessage.OperationType.START));

        for (ECSNode n : toStart) {
            if (multicaster.getErrors().keySet().contains(n)) {
                hashRing.removeNode(n);
            } else {
                n.setStatus(ECSNode.ServerStatus.ACTIVE);
            }
        }

        updateMetadata();
        return ret;
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
        toStop.forEach(n -> n.setStatus(ECSNode.ServerStatus.STOP));

        updateMetadata();
        return ret;
    }

    @Override
    public boolean shutdown() throws Exception {
        ECSMulticaster multicaster = new ECSMulticaster(zk, nodeTable.values()
                .stream().map((n) -> (ECSNode) n).collect(Collectors.toList()));
        boolean ret = multicaster.send(new KVAdminMessage(KVAdminMessage.OperationType.SHUT_DOWN));
        if (ret) {
            hashRing.removeAll();
            nodeTable.values()
                    .forEach(n -> ((ECSNode) n).setStatus(ECSNode.ServerStatus.OFFLINE));
            nodePool.addAll(nodeTable.values());
            nodeTable.clear();
            ret = updateMetadata();
        }
        return ret;
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

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        Collection<IECSNode> nodes = setupNodes(count, cacheStrategy, cacheSize);
        if (nodes == null) return null;

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
                    " > ./logs/output.log 2> ./logs/err.log &";
            // Redirect output to files so that ssh channel will not wait for further output
            try {
                logger.info("Executing command: " + sshCmd);
                Process p = Runtime.getRuntime().exec(sshCmd);
                p.waitFor();
                assert !p.isAlive();
                assert p.exitValue() == 0;
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
            ack = awaitNodes(count, ZK_TIMEOUT);
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
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        if (count > nodePool.size()) return null;

        List<IECSNode> nodeList = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            ECSNode n = (ECSNode) nodePool.poll();
            nodeList.add(n);
        }

        byte[] metadata = new Gson().toJson(new ServerMetaData(cacheStrategy, cacheSize)).getBytes();

        // create corresponding Z-nodes on zookeeper server
        try {
            if (zk.exists(ZK_SERVER_ROOT, false) == null) {
                zk.create(ZK_SERVER_ROOT, "".getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }

            for (IECSNode n : nodeList) {
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

        assert nodeList.size() == count;
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
        toWait.forEach(n -> n.setStatus(ECSNode.ServerStatus.STOP));
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

        rearrangeDataStorage(toRemove.stream()
                .filter(n -> n.getStatus().equals(ECSNode.ServerStatus.ACTIVE))
                .collect(Collectors.toList()));

        ECSMulticaster multicaster = new ECSMulticaster(zk, toRemove);
        boolean ret;
        try {
            ret = multicaster.send(new KVAdminMessage(KVAdminMessage.OperationType.SHUT_DOWN));
        } catch (InterruptedException e) {
            e.printStackTrace();
            ret = false;
        }

        if (ret) {
            for (ECSNode n : toRemove) {
                if (n.getStatus().equals(ECSNode.ServerStatus.ACTIVE))
                    hashRing.removeNode(n);

                n.setStatus(ECSNode.ServerStatus.OFFLINE);
                nodeTable.remove(n.getNodeName());
            }
            nodePool.addAll(toRemove);

            ret = updateMetadata();
        }
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
    private String getHashRingJson() {
        List<RawECSNode> activeNodes = nodeTable.values().stream()
                .map(n -> (ECSNode) n)
                .filter(n -> n.getStatus().equals(ECSNode.ServerStatus.ACTIVE))
                .map(RawECSNode::new)
                .collect(Collectors.toList());

        return new Gson().toJson(activeNodes);
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

    /**
     * for nodes that are ACTIVE:
     * data are transferred from them to nearest valid node
     * <p>
     * for nodes that are STOP:
     * data are transferred to them from nearest valid node
     *
     * @param nodes ECSNodes
     * @return successful or not
     */
    private boolean rearrangeDataStorage(Collection<ECSNode> nodes) {
        Set<ECSNode> removeSet = nodes.stream()
                .filter(n -> n.getStatus().equals(ECSNode.ServerStatus.ACTIVE))
                .collect(Collectors.toSet());
        Set<ECSNode> newSet = nodes.stream()
                .filter(n -> n.getStatus().equals(ECSNode.ServerStatus.STOP))
                .collect(Collectors.toSet());

        for (ECSNode node : nodes) {
            if (node.getStatus().equals(ECSNode.ServerStatus.ACTIVE)) {
                // active server in storage will be stopped
                // Its data transferring to "next" server available
                ECSNode dest = findNextNodeAvailable(node, n -> !removeSet.contains(n));
                if (dest != null) {
                    logger.debug("Transferring data\nfrom: " + node + "\nto: " +
                            dest);
                    logger.debug("HashRange: " + node.getNodeHashRange()[0] + " -> "
                            + node.getNodeHashRange()[1]);
                    transferData(node, dest, node.getNodeHashRange());
                } else {
                    logger.warn("No server available to accept data from the deletion of node " + node);
                    logger.warn("The node is the last node active, service down...");
                }
            } else if (node.getStatus().equals(ECSNode.ServerStatus.STOP)) {
                // Stopped server will be activated
                // Its next server will transfer data to it
                ECSNode source = findNextNodeAvailable(node, n -> !newSet.contains(n));
                if (source != null) {
                    transferData(source, node, node.getNodeHashRange());
                } else {
                    logger.warn("No server exist yet. No data to transfer to node " + node);
                }
            } else {
                // Report error
                logger.error("Invalid node status for data rearrangement " + node);
            }
        }
        return true;
    }

    private ECSNode findNextNodeAvailable(ECSNode node, Predicate<ECSNode> condition) {
        ECSNode dest = node;
        Integer loopCounter = 0;
        while (true) {
            // Get next node on HashRing
            dest = hashRing.getNextNode(dest);
            if (condition.test(dest))
                return dest;
            if (dest.equals(node)) {
                // HashRing exhausted
                return null;
            }
            loopCounter++;
            if (loopCounter > 2 * hashRing.getSize()) {
                throw new ECSHashRing.HashRingException(ECSHashRing.LOOP_ERROR_STR);
            }
        }
    }

    private boolean transferData(ECSNode from, ECSNode to, String[] hashRange) {
        assert from != null;
        assert to != null;
        assert hashRange != null;
        assert hashRange.length == 2;
        try {
            ECSMulticaster multicaster = new ECSMulticaster(zk, Collections.singletonList(to));
            boolean ack = multicaster.send(new KVAdminMessage(KVAdminMessage.OperationType.RECEIVE));

            if (!ack) {
                logger.error("Failed to ack receiver of data " + to);
                logger.error("hash range is " + hashRange[0] + " to " + hashRange[1]);
                return false;
            }

            logger.info("Confirmed receiver node " + to);

            multicaster = new ECSMulticaster(zk, Collections.singletonList(from));
            KVAdminMessage message = new KVAdminMessage(KVAdminMessage.OperationType.SEND);
            message.setReceiverHost(to.getNodeHost());
            message.setReceiverName(to.getNodeName());
            message.setHashRange(hashRange);

            ack = multicaster.send(message);

            if (!ack) {
                logger.error("Failed to ack sender of data " + from);
                logger.error("hash range is " + hashRange[0] + " to " + hashRange[1]);
                return false;
            }

            logger.info("Confirmed sender node " + from);

            // Start listening sender's progress
            ack = new ECSTransferListener(zk, from, to).start();
            if (!ack) {
                logger.error("Failed in data transferring");
                return false;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
}

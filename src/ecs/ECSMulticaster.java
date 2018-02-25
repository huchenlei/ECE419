package ecs;

import common.messages.KVAdminMessage;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static ecs.ECS.ZK_TIMEOUT;

public class ECSMulticaster implements Watcher {
    private static Logger logger = Logger.getRootLogger();
    private ZooKeeper zk;
    private CountDownLatch sig;

    private Collection<ECSNode> nodes;
    private Map<ECSNode, String> errors;
    private Map<String, String> rawErrors;

    private static long timestamp = 0;

    private String logMsg;

    public ECSMulticaster(ZooKeeper zk, Collection<ECSNode> nodes) {
        this.zk = zk;
        this.nodes = nodes;
        this.sig = new CountDownLatch(nodes.size());
        this.errors = new HashMap<>();
        this.rawErrors = new HashMap<>();
    }

    public boolean send(KVAdminMessage msg) throws InterruptedException {
        logMsg = "Receive node deletion, message receive confirmed " + msg;
        for (ECSNode n : nodes) {
            String msgPath = ECS.getNodePath(n) + "/message" + timestamp;
            timestamp++;
            try {
                zk.create(msgPath, msg.encode().getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                Stat exists = zk.exists(msgPath, this);
                if (exists == null) {
                    sig.countDown();
                    logger.debug(logMsg);
                }
            } catch (KeeperException e) {
                errors.put(n, "issue encountered create msg node " + msgPath +
                        "\n" + e.getMessage() + "\n" + Arrays.toString(e.getStackTrace()));
            }
        }

        boolean sigWait = sig.await(ZK_TIMEOUT, TimeUnit.MILLISECONDS);
        // Convert raw errors to map actual ecs node causing them
        for (String msgPath : rawErrors.keySet()) {
            boolean sourceFound = false;
            for (ECSNode n : nodes) {
                if (msgPath.contains(n.getNodeName())) {
                    errors.put(n, rawErrors.get(msgPath));
                    sourceFound = true;
                    break;
                }
            }
            assert sourceFound;
        }
        // Report errors
        for (Map.Entry<ECSNode, String> entry : errors.entrySet()) {
            logger.error(entry.getKey() + ": " + entry.getValue());
        }
        return sigWait && (rawErrors.size() == 0);
    }

    public Map<ECSNode, String> getErrors() {
        return errors;
    }

    @Override
    public void process(WatchedEvent event) {
        sig.countDown();
        String error = null;
        switch (event.getType()) {
            case NodeDeleted:
                // The message is received and properly handled by the server
                logger.debug(logMsg);
                break;
            case NodeDataChanged:
                try {
                    error = new String(zk.getData(event.getPath(), false, null));
                } catch (KeeperException e) {
                    error = "issue encountered querying error message at node " + event.getPath() +
                            "\n" + e.getMessage() + "\n" + Arrays.toString(e.getStackTrace());
                } catch (InterruptedException e) {
                    error = "Got interrupted retrieving error message at node " + event.getPath();
                }
                break;
            default:
                error = "Unexpected type received: " + event.getType()
                        + " from node " + event.getPath();
        }
        if (error != null)
            rawErrors.put(event.getPath(), error);
    }
}

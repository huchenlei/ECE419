package ecs;

import com.google.gson.Gson;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import server.ServerMetaData;

public class ECSTransferListener implements Watcher {
    // total timeout 2 hours
    public static final Integer MAX_TIMEOUT = 2 * 3600 * 1000;
    // 2s between 2 updates
    public static final Integer TIMEOUT = 2 * 1000;

    private static Logger logger = Logger.getRootLogger();
    private ZooKeeper zk;
    private ECSNode sender;
    private ECSNode receiver;

    private boolean senderComplete = false;
    private boolean receiverComplete = false;

    private Integer senderProgress = 0;
    private Integer receiverProgress = 0;

    private String prompt;

    public ECSTransferListener(ZooKeeper zk, ECSNode sender, ECSNode receiver) {
        this.zk = zk;
        this.sender = sender;
        this.receiver = receiver;
        this.prompt = sender.getNodeName() + "->" + receiver.getNodeName() + ": ";
    }

    public boolean start() throws InterruptedException {
        try {
            zk.exists(ECS.getNodePath(sender), this);
        } catch (KeeperException e) {
            logger.error(e.getMessage());
            logger.error(e.getPath() + " : " + e.getResults());
            return false;
        }

        while (true) {
            Integer ps = senderProgress;
            Integer pr = receiverProgress;

            this.wait(TIMEOUT);

            if (senderComplete && receiverComplete) {
                // Complete
                return true;
            } else if (receiverProgress.equals(ps) && senderProgress.equals(pr)) {
                // No data change
                // Must be a timeout
                logger.error("TIMEOUT triggered before receiving any progress on data transferring");
                logger.error("final progress " + senderProgress + "%");
                return false;
            }
        }
    }

    private ServerMetaData parseServerData(byte[] data) {
        return new Gson().fromJson(new String(data), ServerMetaData.class);
    }

    private void checkReceiver() throws KeeperException, InterruptedException {
        ServerMetaData recvData = parseServerData(
                zk.getData(ECS.getNodePath(receiver), false, null));
        receiverProgress = recvData.getTransferProgress();
        if (recvData.isIdle()) {
            receiverComplete = true;
            logger.info(prompt + "receiver side complete");
        } else {
            zk.exists(ECS.getNodePath(receiver), this);
        }
        this.notify();
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType().equals(Event.EventType.NodeDataChanged)) {
            try {
                if (!senderComplete) {
                    // Monitor sender
                    byte[] data = zk.getData(ECS.getNodePath(sender), false, null);
                    ServerMetaData metadata = parseServerData(data);
                    senderProgress = metadata.getTransferProgress();
                    logger.info(prompt + senderProgress + "%");

                    if (metadata.isIdle()) {
                        // Sender complete, now monitoring receiver
                        senderComplete = true;
                        logger.info(prompt + "sender side complete");
                        checkReceiver();
                    } else {
                        // Continue listing for sender progress
                        zk.exists(ECS.getNodePath(sender), this);
                        this.notify();
                    }
                } else if (!receiverComplete) {
                    checkReceiver();
                }
            } catch (KeeperException e) {
                logger.error(e.getMessage());
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else {
            logger.warn("Other unexpected event monitored " + event);
            logger.warn("Continue listening for progress");
        }
    }
}

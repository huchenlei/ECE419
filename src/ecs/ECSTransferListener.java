package ecs;

import com.google.gson.Gson;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import server.ServerMetaData;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ECSTransferListener implements Watcher {
    // total timeout 2 hours
    public static final Integer MAX_TIMEOUT = 2 * 3600 * 1000;
    // 2s between 2 updates
    public static final Integer TIMEOUT = 10 * 1000;

    private static Logger logger = Logger.getRootLogger();
    private ZooKeeper zk;
    private ECSNode sender;
    private ECSNode receiver;

    private boolean senderComplete = false;
    private boolean receiverComplete = false;

    private Integer senderProgress = -1;
    private Integer receiverProgress = -1;

    private String prompt;

    private CountDownLatch sig = null;

    public ECSTransferListener(ZooKeeper zk, ECSNode sender, ECSNode receiver) {
        this.zk = zk;
        this.sender = sender;
        this.receiver = receiver;
        this.prompt = sender.getNodeName() + "->" + receiver.getNodeName() + ": ";
    }

    public boolean start() throws InterruptedException {
        try {
            checkSender();
            if (senderComplete && receiverComplete) {
                logger.info(prompt + "transmission complete");
                return true;
            }
            zk.exists(ECS.getNodePath(sender), this);
        } catch (KeeperException e) {
            logger.error(e.getMessage());
            logger.error(e.getPath() + " : " + e.getResults());
            return false;
        }

        while (true) {
            Integer psender = senderProgress;
            Integer preciver = receiverProgress;

            sig = new CountDownLatch(1);
            sig.await(TIMEOUT, TimeUnit.MILLISECONDS);

            if (senderComplete && receiverComplete) {
                // Complete
                return true;
            } else if (receiverProgress.equals(preciver)
                    && senderProgress.equals(psender)) {
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
        if (receiverProgress.equals(recvData.getTransferProgress())) {
            zk.exists(ECS.getNodePath(receiver), this);
            return;
        }

        receiverProgress = recvData.getTransferProgress();
        if (recvData.isIdle()) {
            receiverComplete = true;
            logger.info(prompt + "receiver side complete");
        } else {
            zk.exists(ECS.getNodePath(receiver), this);
        }
        if (sig != null) sig.countDown();
    }

    private void checkSender() throws KeeperException, InterruptedException {
        // Monitor sender
        byte[] data = zk.getData(ECS.getNodePath(sender), false, null);
        ServerMetaData metadata = parseServerData(data);
        if (senderProgress.equals(metadata.getTransferProgress())) {
            zk.exists(ECS.getNodePath(sender), this);
            return;
        }
        senderProgress = metadata.getTransferProgress();
        logger.info(prompt + senderProgress + "%");


        if (metadata.isIdle()) {
            // Sender complete, now monitoring receiver
            senderComplete = true;
            logger.info(prompt + "sender side complete");
            checkReceiver();
        } else {
            // Continue listening for sender progress
            zk.exists(ECS.getNodePath(sender), this);
            if (sig != null) sig.countDown();
        }
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType().equals(Event.EventType.NodeDataChanged)) {
            try {
                if (!senderComplete) {
                    checkSender();
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

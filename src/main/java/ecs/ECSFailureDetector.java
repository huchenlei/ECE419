package ecs;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class ECSFailureDetector implements Watcher {
    private static Logger logger = Logger.getRootLogger();
    private String nodeName;
    private ECS ecs;

    public ECSFailureDetector(ECS ecs, String name) {
        this.ecs = ecs;
        this.nodeName = name;
    }

    @Override
    public synchronized void process(WatchedEvent event) {
        switch (event.getType()) {
            case NodeDeleted:
                logger.warn(nodeName + " shutdown detected");
                switch (ecs.getNodeByName(nodeName).status) {
                    case ACTIVE:
                        logger.error("Unexpected server shutdown, moving server from active list");
                        boolean ret = ecs.handleNodeShutDown(nodeName);
                        if (!ret)
                            logger.fatal("Unable to remove node " + nodeName + " from ecs!");
                        break;
                    case STOP:
                        logger.error("Unexpected stopped node shutdown");
                        ecs.handleStopNodeShutDown(nodeName);
                        break;
                }
                break;
            default:
                logger.warn(event.getType() + " received on " + nodeName);
        }
    }
}

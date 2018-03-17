package testing;

import ecs.ECSDataDistributionManager;
import ecs.ECSDataTransferIssuer;
import ecs.ECSHashRing;
import ecs.ECSNode;
import junit.framework.TestCase;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class DataDistributionManagerTest extends TestCase {
    static private Logger logger = Logger.getRootLogger();
    private ECSHashRing hashRing;
    private List<ECSNode> sampleNodes;
    private static final Integer NODE_NUM = 10;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        this.hashRing = new ECSHashRing();
        this.sampleNodes = new ArrayList<>();

        for (int i = 0; i < NODE_NUM; i++) {
            sampleNodes.add(new ECSNode("node" + i, "127.0.0.1", 5000 + i));
            hashRing.addNode(sampleNodes.get(i));
        }
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        this.hashRing.removeAll();
        this.hashRing = null;
    }

    public void testAddNodeBasic() {
        hashRing = new ECSHashRing();
        ECSDataDistributionManager manager = new ECSDataDistributionManager(hashRing);
        List<List<ECSDataTransferIssuer>> results = new ArrayList<>();

        for (ECSNode sampleNode : sampleNodes) {
            List<ECSDataTransferIssuer> ecsDataTransferIssuers = manager.addNode(sampleNode);
            results.add(ecsDataTransferIssuers);
            logger.info(sampleNode.getNodeName() + " add to hash ring");
            ecsDataTransferIssuers.forEach(logger::info);
            logger.info("--------------------------------------------");

            hashRing.addNode(sampleNode);
        }

        assertEquals(0, results.get(0).size());
        assertEquals(2, results.get(1).size());
        assertEquals(sampleNodes.get(0), results.get(1).get(0).getSender());

        for (int i = 2; i < results.size(); i++) {
            assertEquals(3, results.get(i).size());
        }
    }

    public void testRemoveNodeBasic() {
        ECSDataDistributionManager manager = new ECSDataDistributionManager(hashRing);
        List<List<ECSDataTransferIssuer>> results = new ArrayList<>();

        for (ECSNode sampleNode : sampleNodes) {
            List<ECSDataTransferIssuer> ecsDataTransferIssuers = manager.removeNode(sampleNode);
            results.add(ecsDataTransferIssuers);
            logger.info(sampleNode.getNodeName() + " remove from hash ring");
            ecsDataTransferIssuers.forEach(logger::info);
            logger.info("--------------------------------------------");

            hashRing.removeNode(sampleNode);
        }

        for (int i = 0; i < results.size() - 3; i++) {
            assertEquals(3, results.get(i).size());
        }

        for (int i = results.size() - 3; i < results.size(); i++) {
            assertEquals(0, results.get(i).size());
        }
    }
}

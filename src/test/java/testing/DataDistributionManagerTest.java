package testing;

import ecs.ECSDataDistributionManager;
import ecs.ECSDataTransferIssuer;
import ecs.ECSHashRing;
import ecs.ECSNode;
import junit.framework.TestCase;
import org.apache.log4j.Logger;

import java.lang.reflect.Field;
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
            ECSNode sample = new ECSNode("node" + i, "127.0.0.1", 5000 + i);
            Field hashField = ECSNode.class.getSuperclass().getDeclaredField("hash");
            hashField.setAccessible(true);
            hashField.set(sample, String.valueOf(1000 * i));
            sampleNodes.add(sample);
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
        }

        assertEquals(0, results.get(0).size());

        for (int i = 1; i < ECSHashRing.REPLICATION_NUM + 1; i++) {
            assertEquals(1, results.get(i).size());
        }

        for (int i = ECSHashRing.REPLICATION_NUM + 1; i < results.size(); i++) {
            assertEquals(4, results.get(i).size());
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
        }

        for (int i = 0; i < results.size() - 3; i++) {
            assertEquals(3, results.get(i).size());
        }

        for (int i = results.size() - 3; i < results.size(); i++) {
            assertEquals(0, results.get(i).size());
        }
    }
}

package testing;

import ecs.ECSDataDistributionManager;
import ecs.ECSDataTransferIssuer;
import ecs.ECSHashRing;
import ecs.ECSNode;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.List;

public class DataDistributionManagerTest extends TestCase {
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

    public void testAddBasicNode() {
        hashRing = new ECSHashRing();
        ECSDataDistributionManager manager = new ECSDataDistributionManager(hashRing);
        List<List<ECSDataTransferIssuer>> results = new ArrayList<>();

        for (ECSNode sampleNode : sampleNodes) {
            results.add(manager.addNode(sampleNode));
            hashRing.addNode(sampleNode);
        }

        assertEquals(0, results.get(0).size());
        assertEquals(1, results.get(1).size());
        assertEquals(sampleNodes.get(0), results.get(1).get(0).getSender());

    }
}

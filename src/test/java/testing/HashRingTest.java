package testing;

import common.messages.KVMessage;
import ecs.ECSHashRing;
import ecs.ECSNode;
import junit.framework.TestCase;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import performance.DataParser;

import java.util.Collection;
import java.util.List;

public class HashRingTest extends TestCase {
    private static Logger logger = Logger.getRootLogger();
    private Exception ex;
    private static ECSHashRing hr = new ECSHashRing();

    public void setUp() throws Exception {
        super.setUp();
        if (!LogSetup.isActive()) {
            new LogSetup("./logs/test_ecs.log", Level.ALL);
        }
    }

    private static List<KVMessage> msgs =
            DataParser.parseDataFrom("allen-p/sent");

    @Before
    public void preTest() {
        ex = null;
    }

    public void testAddNode() {
        hr.addNode(new ECSNode("n1", "localhost", 5000));
        hr.addNode(new ECSNode("n2", "localhost", 5001));
        hr.addNode(new ECSNode("n3", "localhost", 5002));

        try {
            // Duplicated hash val should report error
            hr.addNode(new ECSNode("n3", "localhost", 5000));
        } catch (ECSHashRing.HashRingException e) {
            ex = e;
        }

        assertNotNull(ex);
    }


    public void testGetNode() {
        ECSNode n = new ECSNode("n4", "localhost", 5000);

        ECSNode ret = hr.getNodeByKey(n.getNodeHash());
        assertEquals(n, ret);

        ret = hr.getNodeByKey(n.getNodeHash() + 1);
        assertFalse(ret.equals(n));

        ECSNode next = hr.getNextNode(n);
        assertNotNull(next);

        Collection<ECSNode> replications = hr.getReplicationNodes(n);
        assertEquals(new Integer(2), ECSHashRing.REPLICATION_NUM);
        assertEquals(ECSHashRing.REPLICATION_NUM, new Integer(replications.size()));
    }

    public void testRemoveNode() {
        ECSNode n = new ECSNode("n5", "localhost", 5000);

        hr.removeNode(n);

        try {
            hr.removeNode(n);
        } catch (ECSHashRing.HashRingException e) {
            ex = e;
        }
        assertNotNull(ex);

        ECSNode n2 = hr.getNodeByKey(n.getNodeHash());
        assertFalse(n.equals(n2));
    }

    public void testMassiveKeyCompare() {
        hr.addNode(new ECSNode("n6", "localhost", 50000));
        hr.addNode(new ECSNode("n7", "localhost", 50001));

        logger.info(hr);

        for (KVMessage msg : msgs) {
            String hash = ECSNode.calcHash(msg.getKey());
            logger.debug("hash is " + hash);

            ECSNode node = hr.getNodeByKey(hash);
            logger.debug("node found is " + node);
            logger.debug("lower bound: " + node.getNodeHashRange()[0]);
            logger.debug("higher bound: " + node.getNodeHashRange()[1]);

            assertTrue(ECSNode.isKeyInRange(msg.getKey(), node.getNodeHashRange()));
        }
    }

    public void testHashRangeBasic() {
        ECSNode.HashRange range1 = new ECSNode.HashRange("100", "200");
        ECSNode.HashRange range2 = new ECSNode.HashRange("150", "250");

        String[] intersection = range1.intersection(range2).getStringRange();
        assertEquals( "150", intersection[0]);
        assertEquals("200", intersection[1]);

        String[] union = range1.union(range2).getStringRange();
        assertEquals("100", union[0]);
        assertEquals("250", union[1]);

        List<ECSNode.HashRange> remove1 = range1.remove(range2);
        assertEquals(1, remove1.size());
        String[] remove1_0 = remove1.get(0).getStringRange();

        assertEquals("100", remove1_0[0]);
        assertEquals("150", remove1_0[1]);

        ECSNode.HashRange range3 = new ECSNode.HashRange("300", "400");
        ECSNode.HashRange range4 = new ECSNode.HashRange("480", "500");
        ECSNode.HashRange bigRange = new ECSNode.HashRange("0", "500");
    }
}

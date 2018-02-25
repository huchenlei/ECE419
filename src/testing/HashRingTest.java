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
            DataParser.parseDataFrom("allen-p");

    @Before
    public void preTest() {
        ex = null;
    }

    public void testAddNode() {
        hr.addNode(new ECSNode("n1", "localhost", 5000));
        hr.addNode(new ECSNode("n2", "localhost", 5001));

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
}

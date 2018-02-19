package testing;

import ecs.ECSHashRing;
import ecs.ECSNode;
import junit.framework.TestCase;
import org.junit.Before;

public class HashRingTest extends TestCase {
    private Exception ex;
    private static ECSHashRing hr = new ECSHashRing();

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
}

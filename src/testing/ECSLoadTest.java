package testing;

import app_kvServer.KVServer;
import client.KVStore;
import common.KVMessage;
import ecs.ECS;
import ecs.ECSNode;
import ecs.IECSNode;
import junit.framework.TestCase;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;
import performance.DataParser;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static testing.ECSLoadTest.TestHelper.addNodes;
import static testing.ECSLoadTest.TestHelper.getRandomClient;
import static testing.ECSLoadTest.TestHelper.testGetData;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ECSLoadTest extends TestCase {
    private static Logger logger = Logger.getRootLogger();
    private static final String CACHE_STRATEGY = "FIFO";
    private static final Integer CACHE_SIZE = 32;

    public void setUp() throws Exception {
        super.setUp();
        if (!LogSetup.isActive()) {
            new LogSetup("./logs/test_ecs.log", Level.ALL);
        }
    }

    private static ECS ecs = null;
    private Exception ex = null;
    private static Map<ECSNode, KVServer> serverTable = new HashMap<>();
    private static List<KVStore> clients = new ArrayList<>();
    private static List<KVMessage> msgs = DataParser.parseDataFrom("allen-p/inbox");

    static class TestHelper {
        static final Integer ACCESS_NUM = 64;

        static KVStore getRandomClient() {
            assert clients != null;
            return clients.get(new Random().nextInt(clients.size() - 1));
        }

        /**
         * Request data from servers
         */
        static void testGetData() throws Exception {
            for (int i = 0; i < ACCESS_NUM; i++) {
                int randIndex = new Random().nextInt(msgs.size() - 1);
                KVMessage msg = msgs.get(randIndex);
                KVStore client = getRandomClient();
                KVMessage ret = client.get(msg.getKey());
                assertEquals(KVMessage.StatusType.GET_SUCCESS, ret.getStatus());
                assertEquals(msg.getValue(), ret.getValue());
            }
        }

        static void addNodes(Integer count) throws Exception {
            Collection<IECSNode> nodes = ecs.setupNodes(count, CACHE_STRATEGY, CACHE_SIZE);
            assertNotNull(nodes);
            assertEquals(count, new Integer(nodes.size()));
            // Start the servers internally
            for (IECSNode node : nodes) {
                KVServer server = new KVServer(node.getNodePort(), node.getNodeName(),
                        ECS.ZK_HOST, Integer.parseInt(ECS.ZK_PORT));
                serverTable.put((ECSNode) node, server);
                server.clearStorage();
                new Thread(server).start();
            }
            boolean ret = ecs.awaitNodes(count, ECS.ZK_TIMEOUT);
            assertTrue(ret);
        }
    }

    @Before
    public void preTest() {
        ex = null;
    }

    /*
    Following are advanced testcases for ECS functionality
    User actions (put/get) data is simulated to test the robustness of the system
     */

    /**
     * Create the ECS object
     */
    public void test01Creation() throws IOException {
        ecs = new ECS("./ecs.config");
        assertNotNull(ecs);
    }

    /**
     * Add several nodes to ECS
     */
    public void test02AddNodes() throws Exception {
        addNodes(5);
        for (KVServer server : serverTable.values()) {
            KVStore store = new KVStore(server.getHostname(), server.getPort());
            store.connect();
            KVMessage message = store.put("test", "test string");
            assertEquals(KVMessage.StatusType.SERVER_STOPPED, message.getStatus());

            clients.add(store);
        }
    }

    /**
     * Start the nodes just added
     * Up to this point the service shall be online
     * All KVServers should be accepting client requests
     */
    public void test03StartNodes() throws Exception {
        boolean ret = ecs.start();
        assertTrue(ret);
        Thread.sleep(100); // wait for server to init hashRing
    }

    /**
     * Populate the servers with enron dataset
     */
    public void test04PutData() throws Exception {
        for (KVMessage msg : msgs) {
            KVStore client = getRandomClient();
            KVMessage ret = client.put(msg.getKey(), msg.getValue());
            assertEquals(KVMessage.StatusType.PUT_SUCCESS, ret.getStatus());
        }
        // Confirm data stored
        testGetData();
    }

    /**
     * Remove few of the servers from
     */
    public void test05RemoveNodes() throws Exception {
        ArrayList<ECSNode> nodes = new ArrayList<>(serverTable.keySet());
        // Remove the first two nodes
        List<ECSNode> toRemove = nodes.subList(0, 2);
        boolean ret = ecs.removeNodes(
                toRemove.stream().map(ECSNode::getNodeName)
                        .collect(Collectors.toList()));
        assertTrue(ret);
        toRemove.forEach(serverTable::remove);
        testGetData();
    }

    /**
     * Stop the service and restart it
     */
    public void test06StopNodes() throws Exception {
        boolean ret = ecs.stop();
        assertTrue(ret);

        assert clients.size() > 0;
        assert msgs.size() > 0;
        KVMessage m = clients.get(0).get(msgs.get(0).getKey());
        assertEquals(KVMessage.StatusType.SERVER_STOPPED, m.getStatus());

        // restart the servers should recover the service
        ret = ecs.start();
        assertTrue(ret);
        Thread.sleep(100);

        testGetData();
    }

    /**
     * Add nodes when there is existing service online
     */
    public void test07AddNodesExisting() throws Exception {
        addNodes(2);
        ecs.start();
        testGetData();
    }


    /**
     * Shutdown the whole service
     */
    public void test08Shutdown() throws Exception {
        boolean ret = ecs.shutdown();
        assertTrue(ret);
    }
}

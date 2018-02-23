package testing;

import app_kvServer.KVServer;
import client.KVStore;
import common.messages.KVMessage;
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

import static testing.ECSLoadTest.TestHelper.getRandomClient;
import static testing.ECSLoadTest.TestHelper.testGetData;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ECSLoadTest extends TestCase {
    private static Logger logger = Logger.getRootLogger();
    private static final String CACHE_STRATEGY = "FIFO";
    private static final Integer CACHE_SIZE = 1024;

    public void setUp() throws Exception {
        super.setUp();
        if (!LogSetup.isActive()) {
            new LogSetup("./logs/test_ecs.log", Level.ALL);
        }
    }

    public void tearDown() throws Exception {
        super.tearDown();
        for (KVServer server : serverTable.values()) {
            server.clearStorage();
        }
    }

    private static ECS ecs = null;
    private Exception ex = null;
    private static Map<ECSNode, KVServer> serverTable = new HashMap<>();
    private static List<KVStore> clients = new ArrayList<>();
    private static List<KVMessage> msgs = DataParser.parseDataFrom("arora-h");

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
        Integer count = 5;
        Collection<IECSNode> nodes = ecs.setupNodes(count, CACHE_STRATEGY, CACHE_SIZE);
        assertNotNull(nodes);
        assertEquals(count, new Integer(nodes.size()));
        // Start the servers internally
        for (IECSNode node : nodes) {
            KVServer server = new KVServer(node.getNodeName(), ECS.ZK_HOST, Integer.parseInt(ECS.ZK_PORT));
            serverTable.put((ECSNode) node, server);
            new Thread(server).start();
        }

        boolean ret = ecs.awaitNodes(count, ECS.ZK_TIMEOUT);
        assertTrue(ret);

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

    public static class TestHelper {
        public static KVStore getRandomClient() {
            assert clients != null;
            return clients.get(new Random().nextInt(clients.size() - 1));
        }

        /**
         * Request data from servers
         */
        public static void testGetData() throws Exception {
            for (KVMessage msg : msgs) {
                KVStore client = getRandomClient();
                KVMessage ret = client.get(msg.getKey());
                assertEquals(ret.getValue(), msg.getValue());
                assertEquals(ret.getStatus(), KVMessage.StatusType.GET_SUCCESS);
            }
        }
    }
}

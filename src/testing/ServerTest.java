package testing;

import app_kvServer.KVServer;
import common.KVMessage;
import ecs.ECS;
import ecs.ECSNode;
import junit.framework.TestCase;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;
import performance.DataParser;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import static ecs.ECS.ZK_SERVER_ROOT;

public class ServerTest extends TestCase {

    private static List<KVMessage> msgs = DataParser.parseDataFrom("allen-p/inbox");

    @Override
    public void setUp() throws Exception {
        super.setUp();
        if (!LogSetup.isActive()) {
            new LogSetup("./logs/test_server.log", Level.ALL);
        }

    }

    @Test
    public void testMoveData() throws Exception {
        ZooKeeper zk;
        CountDownLatch sig = new CountDownLatch(0);

        zk = new ZooKeeper(ECS.ZK_CONN, ECS.ZK_TIMEOUT, event -> {
            if (event.getState().equals(Watcher.Event.KeeperState.SyncConnected)) {
                // connection fully established can proceed
                sig.countDown();
            }
        });
        try {
            sig.await();
        } catch (InterruptedException e) {
            // Should never happen
            e.printStackTrace();
        }

        if (zk.exists(ZK_SERVER_ROOT, false) == null) {
            zk.create(ZK_SERVER_ROOT, "".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        KVServer server1 = new KVServer("TestServer1", "localhost", 2181);
        server1.clearStorage();

        KVServer server2 = new KVServer("TestServer2", "localhost", 2181);
        server2.clearStorage();

        String hashString1 = "358343938402ebb5110716c6e836f5a2";
        String hashString2 = "a98109598267087dfc364fae4cf24578";
        String hashString3 = "b3638a32c297f43aa37e63bbd839fc7e";
        String[] hashRange1 = new String[2];
        hashRange1[0] = hashString1;
        hashRange1[1] = hashString2;

        String[] hashRange2 = new String[2];
        hashRange2[0] = hashString2;
        hashRange2[1] = hashString3;

        for (KVMessage msg : msgs) {
            if (ECSNode.isKeyInRange(msg.getKey(), hashRange1)) {
                server1.putKV(msg.getKey(), msg.getValue());
            }
            else if (ECSNode.isKeyInRange(msg.getKey(), hashRange2)) {
                server2.putKV(msg.getKey(), msg.getValue());
            }
        }

        new Thread(server1).start();
        boolean result = server2.moveData(hashRange2, "TestServer1");
        assertTrue(result);

        String value1;
        String value2;
        for (KVMessage msg : msgs) {
            String key = msg.getKey();
            value1 = server1.getKV(key);
            value2 = server2.getKV(key);
            if (ECSNode.isKeyInRange(key, hashRange1) || (ECSNode.isKeyInRange(key, hashRange2))) {
                assertEquals(msg.getValue(), value1);
            }
            else{
                assertNull(value1);
            }
            if (value2 != null) {
                assertNull(value2);
            }
        }




    }
}

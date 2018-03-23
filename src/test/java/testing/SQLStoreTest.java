package testing;

import ecs.ECS;
import junit.framework.TestCase;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;
import server.KVIterateStore;
import server.sql.SQLException;
import server.sql.SQLIterateStore;
import server.sql.SQLPersistentStore;
import server.sql.SQLTable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SQLStoreTest extends TestCase {
    private static List<SQLPersistentStore> stores = new ArrayList<>();

    public void test01Creation() throws IOException {
        CountDownLatch sig = new CountDownLatch(1);
        ZooKeeper zk = new ZooKeeper(ECS.ZK_CONN, ECS.ZK_TIMEOUT, event -> {
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

        for (int i = 0; i < 5; i++) {
            KVIterateStore kvStore = new KVIterateStore("SQL_Store" + i);
            stores.add(new SQLIterateStore("sqlstore" + i, zk, kvStore));
        }
    }

    public void test02CreateTable() throws InterruptedException {
        assertTrue(stores.size() > 0);
        Map<String, Class> meta = new HashMap<>();
        meta.put("age", Double.class);
        meta.put("name", String.class);
        meta.put("weight", Double.class);

        stores.get(0).createTable("student", meta);

        Thread.sleep(100);

        Exception ex = null;
        try {
            stores.get(1).createTable("student", meta);
        } catch (SQLException e) {
            ex = e;
        }
        assertNotNull(ex);

        SQLTable student = stores.get(2).getTableByName("student");
        assertNotNull(student);
    }

    public void test03DropTable() throws IOException, InterruptedException {
        stores.get(0).dropTable("student");

        Thread.sleep(100);

        SQLTable student = stores.get(1).getTableByName("student");
        assertNull(student);
    }
}

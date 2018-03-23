package testing;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import ecs.ECS;
import junit.framework.TestCase;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;
import server.KVIterateStore;
import server.sql.SQLExecutor;
import server.sql.SQLIterateStore;
import server.sql.SQLPersistentStore;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SQLExecutorTest extends TestCase {
    private static SQLExecutor executor;
    private static List<SQLPersistentStore> stores = new ArrayList<>();

    @Override
    public void setUp() throws InterruptedException {
        if (stores.size() != 0) {
            int index = new Random().nextInt(4);
            executor = new SQLExecutor(stores.get(index));
        }
        Thread.sleep(100);
    }

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
            kvStore.clearStorage();
            stores.add(new SQLIterateStore("sqlstore" + i, zk, kvStore));
        }
    }

    public void test02CreateTable() {
        String s = executor.executeSQL("create student age:number,name:string");
        assertEquals("", s);
    }

    public void test03Insert() {
        executor = new SQLExecutor(stores.get(0));
        String[] studentNames = {"Alice", "Bob", "Charlie", "David"};
        for (int i = 0; i < studentNames.length; i++) {
            String studentJson = "{'age':" + (i + 10) + ",'name':'" + studentNames[i] + "'}";
            String s = executor.executeSQL("insert " + studentJson + " to student");
            assertEquals("", s);
        }
    }

    private static Type type = new TypeToken<List<Map<String, Object>>>() {
    }.getType();

    public void test04Query() {
        executor = new SQLExecutor(stores.get(0));
        String result = executor.executeSQL("select name,age from student where name = Alice");
        List<Map<String, Object>> rowList = new Gson().fromJson(result, type);
        assertEquals(1, rowList.size());

        Map<String, Object> row = rowList.get(0);
        assertEquals("Alice", row.get("name"));
        assertEquals(10d, row.get("age"));
    }

    public void test05Update() {
        executor = new SQLExecutor(stores.get(0));
        String s = executor.executeSQL("update {'age':100} to student where name = Alice");
        assertEquals(1, Integer.parseInt(s));

        String result = executor.executeSQL("select name from student where age > 50");
        List<Map<String, Object>> rowList = new Gson().fromJson(result, type);
        assertEquals(1, rowList.size());

        Map<String, Object> row = rowList.get(0);
        assertEquals("Alice", row.get("name"));
    }

    public void test06Delete() {
        executor = new SQLExecutor(stores.get(0));
        String s = executor.executeSQL("delete from student where name = Bob");
        assertEquals(1, Integer.parseInt(s));
    }

    public void test07Drop() {
        executor = new SQLExecutor(stores.get(0));
        String s = executor.executeSQL("drop student");
        assertEquals("", s);
    }
}

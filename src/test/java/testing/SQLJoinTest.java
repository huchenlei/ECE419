package testing;

import app_kvServer.KVServer;
import client.KVStore;
import common.messages.KVMessage;
import ecs.ECS;
import ecs.ECSNode;
import junit.framework.TestCase;
import org.apache.log4j.Logger;
import server.sql.SQLIterateTable;
import server.sql.SQLParser;
import server.sql.SQLScanner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static testing.ECSLoadTest.TestHelper.addNodes;

public class SQLJoinTest extends TestCase {
    private static ECS ecs = null;
    private static Map<ECSNode, KVServer> serverTable = new HashMap<>();
    private static Map<String, List<Map<String, Object>>> sqlObjTable = new HashMap<>();
    private static Logger logger = Logger.getRootLogger();

    public void testParseJoin() {
        String sql = "SELECT STUDENT.name,GRADE.math FROM STUDENT " +
                "WHERE age > 10 JOIN GRADE ON STUDENT.id = GRADE.studentID";
        List<SQLScanner.SQLToken> tokens = SQLScanner.scan(sql);
        SQLParser.parse(tokens);

        sql = "SELECT STUDENT.name,GRADE.math FROM STUDENT " +
                "JOIN GRADE ON STUDENT.id = GRADE.studentID";
        tokens = SQLScanner.scan(sql);
        SQLParser.parse(tokens);
    }

    private static String objInsertString(String table, Map<String, Object> obj) {
        return "insert " + SQLIterateTable.mapToJson(obj) + " to " + table;
    }

    private static String objQueryString(String table, Map<String, Object> obj) {
        return "select name,age from " + table + " where name = " + obj.get("name");
    }

    public void testExecuteJoin() throws Exception {
        // create two table
        // student table
        String tableName = "student";
        List<Map<String, Object>> objList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Map<String, Object> obj = new HashMap<>();
            obj.put("name", tableName + i);
            obj.put("age", (double) i + 10);
            obj.put("id", 10000 + i);
            objList.add(obj);
        }
        sqlObjTable.put(tableName, objList);

        tableName = "grade";
        objList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Map<String, Object> obj = new HashMap<>();
            obj.put("ECE419", 10 + i);
            obj.put("math",  i + 80);
            obj.put("studentID", 10000 + i);
            objList.add(obj);
        }
        sqlObjTable.put(tableName, objList);

        // open ecs
        ecs = new ECS("ecs.config");
        ecs.clearRestoreList();
        ecs.locally = true;

        addNodes(15, ecs, "FIFO", 32, serverTable);
        KVServer server = serverTable.entrySet().iterator().next().getValue();
        KVStore store = new KVStore(server.getHostname(), server.getPort());
        store.connect();

        ecs.start();
        Thread.sleep(100); // wait for server to init hashRing

        {
            store.sql("drop " + "student");
            KVMessage ret = store.sql("create student age:number,name:string,id:number");
            assertEquals(KVMessage.StatusType.SQL_SUCCESS, ret.getStatus());

            store.sql("drop " + "grade");
            ret = store.sql("create grade math:number,ECE419:number,studentID:number");
            assertEquals(KVMessage.StatusType.SQL_SUCCESS, ret.getStatus());

        }

        for (Map.Entry<String, List<Map<String, Object>>> entry : sqlObjTable.entrySet()) {
            for (Map<String, Object> obj : entry.getValue()) {
                KVMessage ret = store.sql(objInsertString(entry.getKey(), obj));
                assertEquals(KVMessage.StatusType.SQL_SUCCESS, ret.getStatus());
            }
        }


        Thread.sleep(100);

        String sqlString = "select student.name,grade.math,grade.ECE419 FROM student " +
                "WHERE name = student8 JOIN grade ON student.id = grade.studentID";
        KVMessage ret = store.sql(sqlString);
        logger.info(ret);

        sqlString = "select student.name,grade.math FROM student " +
                "JOIN grade ON student.id = grade.studentID";
        ret = store.sql(sqlString);
        logger.info(ret);

    }
}

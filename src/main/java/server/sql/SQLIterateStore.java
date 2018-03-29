package server.sql;

import com.google.gson.reflect.TypeToken;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import server.KVIterateStore;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

public class SQLIterateStore implements SQLPersistentStore {
    private static Logger logger = Logger.getRootLogger();
    public static final String ZK_TABLE_PATH = "/tables";
    private static Type type = new TypeToken<Map<String, SQLIterateTable>>() {
    }.getType();

    private Map<String, SQLIterateTable> tableMap;
    private ZooKeeper zk;

    private String name;
    private String prompt;
    private KVIterateStore store;

    public SQLIterateStore() {
    }

    public SQLIterateStore(String name, ZooKeeper zk, KVIterateStore store) {
        this.tableMap = new HashMap<>();
        this.zk = zk;
        this.name = name;
        this.prompt = "(" + name + "_SQL_Store):";
        this.store = store;
        updateTablesMetadata();
        try {
            zk.exists(ZK_TABLE_PATH, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getType().equals(Event.EventType.NodeDataChanged)) {
                        syncTablesMetadata(this);
                    }
                }
            });
        } catch (KeeperException | InterruptedException e) {
            logger.error("Unable to register watch on table metadata node");
            logger.error("please check config of zookeeper");
            e.printStackTrace();
        }
    }

    private void syncTablesMetadata(Watcher watcher) {
        try {
            byte[] data;
            if (watcher != null)
                data = zk.getData(ZK_TABLE_PATH, watcher, null);
            else
                data = zk.getData(ZK_TABLE_PATH, false, null);

            tableMap = jsonToSQLMap(new String(data));
            for (SQLIterateTable table : tableMap.values()) {
                table.setStore(store);
            }
            logger.info(prompt + " table map updated");
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private Map<String, SQLIterateTable> jsonToSQLMap(String json) {
        return SQLIterateTable.gson.fromJson(json, type);
    }

    private boolean updateTablesMetadata() {
        byte[] metadata = SQLIterateTable.gson.toJson(tableMap).getBytes();
        try {
            Stat exists = zk.exists(ZK_TABLE_PATH, false);
            if (exists == null) {
                zk.create(ZK_TABLE_PATH, metadata,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } else {
                if (tableMap.size() != 0)
                    zk.setData(ZK_TABLE_PATH, metadata, exists.getVersion());
                else
                    syncTablesMetadata(null);
            }
        } catch (InterruptedException | KeeperException e) {
            logger.error("Unable to update metadata");
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public SQLTable getTableByName(String name) {
        return tableMap.get(name);
    }

    @Override
    public void createTable(String name, Map<String, Class> cols) throws SQLException {
        if (tableMap.get(name) != null)
            throw new SQLException("table " + name + " already exists!");

        SQLIterateTable newTable = new SQLIterateTable(name, store, cols);
        tableMap.put(name, newTable);
        updateTablesMetadata();
    }

    @Override
    public void dropTable(String name) throws SQLException, IOException {
        SQLTable table = tableMap.get(name);
        if (table == null) {
            throw new SQLException("table " + name + " does not exist!");
        }
        table.drop();
        tableMap.remove(name);
        updateTablesMetadata();
    }
}


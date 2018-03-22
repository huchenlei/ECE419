package server.sql;

import java.util.HashMap;
import java.util.Map;

public class SQLIterateStore implements SQLPersistentStore {
    private String dir = "./res/sql";

    public static final String DEFAULT_FILE_PREFIX = "SQL_store";
    private Map<String, SQLTable> tableMap;
    private String filePrefix = DEFAULT_FILE_PREFIX;

    public SQLIterateStore() {
        tableMap = new HashMap<>();
    }

    public SQLIterateStore(String filePrefix) {
        this();
        this.filePrefix = filePrefix;
    }

    @Override
    public SQLTable getTableByName(String name) {
        return tableMap.get(name);
    }

    @Override
    public void createTable(String name, Map<String, Class> cols) throws SQLException {
        if (tableMap.get(name) != null)
            throw new SQLException("table " + name + " already exists!");
    }

    @Override
    public void dropTable(String name) throws SQLException {

    }
}

package server.sql;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SQLExecutor {
    private static Logger logger = Logger.getRootLogger();
    private SQLPersistentStore store;

    public SQLExecutor(SQLPersistentStore store) {
        this.store = store;
    }

    public String executeSQL(String sql) throws IOException {
        return executeSQL(sql, false);
    }

    public String executeSQL(String sql, Boolean isForward) throws IOException {
        List<SQLScanner.SQLToken> tokens = SQLScanner.scan(sql);
        SQLParser.SQLAst ast = SQLParser.parse(tokens);

        if (isForward &&
                Arrays.asList("SELECT", "CREATE", "DROP").contains(ast.action)) {
            // Coordinator only actions
            return "";
        }

        String result = "";
        try {
            switch (ast.action) {
                case "SELECT":
                case "DELETE":
                case "UPDATE":
                case "INSERT":
                    SQLTable table = store.getTableByName(ast.table);
                    if (table == null)
                        throw new SQLException("table " + ast.table + " not found!");
                    switch (ast.action) {
                        case "SELECT":
                            List<Map<String, Object>> query =
                                    table.query(ast.selectCols, ast.conditionFunc);
                            result = SQLIterateTable.gson.toJson(query);
                            break;
                        case "DELETE":
                            result = String.valueOf(table.delete(ast.conditionFunc));
                            break;
                        case "UPDATE":
                            result = String.valueOf(table.update(ast.newVal, ast.conditionFunc));
                            break;
                        case "INSERT":
                            table.insert(ast.newVal);
                            break;
                    }
                    break;
                case "CREATE":
                    store.createTable(ast.table, ast.schema);
                    break;
                case "DROP":
                    store.dropTable(ast.table);
                    break;
            }
        } catch (IOException e) {
            String err = e.getMessage() + ": " + Arrays.toString(e.getStackTrace());
            logger.error(err);
            throw e;
        }

        return result;
    }
}

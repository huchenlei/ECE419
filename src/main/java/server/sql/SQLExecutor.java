package server.sql;

import ecs.ECSHashRing;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

public class SQLExecutor {
    private static Logger logger = Logger.getRootLogger();
    private SQLPersistentStore store;
    private SQLJoinQuerent querent;

    public SQLExecutor(SQLPersistentStore store, SQLJoinQuerent sqlQuerent){
        this.store = store;
        this.querent = sqlQuerent;
    }
    public SQLExecutor(SQLPersistentStore store) {
        this(store, null);
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
                            if (ast.isJoin) {
                                // get the join list
                                List<Object> joinList = new ArrayList<>();
                                for (Map<String, Object> entry: query){
                                    joinList.add(entry.get(ast.onConditionCol));
                                }

                                Map<String, Map<String, Object>> joinQuery;
                                boolean joinTableResp = this.querent.isResponsible(ast.joinTable);
                                if (joinTableResp) {
                                    joinQuery = joinSearch(ast.joinTable, ast.onConditionJoinCol,
                                            joinList, ast.joinSelectCols);
                                }
                                else {
                                    // go to other server
                                    try {
                                        joinQuery = this.querent.queryJoin(ast.joinTable, ast.onConditionJoinCol,
                                                joinList, ast.joinSelectCols);
                                    }
                                    catch (Exception e) {
                                        String err = e.getMessage() + ": " + Arrays.toString(e.getStackTrace());
                                        logger.error(err);
                                        throw new IOException();
                                    }
                                }
                                // integrate the final result
                                List<Map<String, Object>> finalList = new ArrayList<>();
                                for (Map<String, Object> entry: query){
                                    Object joinKey = entry.get(ast.onConditionCol);
                                    Map<String, Object> newEntry = new HashMap<>();
                                    for (String selectCol: ast.selectCols) {
                                        newEntry.put(String.join(".", ast.table, selectCol),
                                                entry.get(selectCol));
                                    }
                                    for (String joinSelectCols: ast.joinSelectCols) {
                                        Map <String, Object> joinEntry = joinQuery.get(joinKey.toString());
                                        if (joinEntry == null) {
                                            logger.error("Join col not found, should not happen");
                                        }
                                        newEntry.put(String.join(".", ast.joinTable, joinSelectCols),
                                                joinEntry.get(joinSelectCols));
                                    }
                                    finalList.add(newEntry);
                                }
                                result = SQLIterateTable.gson.toJson(finalList);
                            }
                            else{
                                result = SQLIterateTable.gson.toJson(query);
                            }
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

    public Map<String, Map<String, Object>> joinSearch(String tableName, String colName, List<Object> valueList,
                                                        List<String> selector) throws IOException {
        Map<String, Map<String, Object>> joinQuery;
        SQLTable joinTable = store.getTableByName(tableName);
        // handle join locally
        joinQuery = joinTable.joinSearch(colName,
                valueList, selector);
        return joinQuery;
    }
}

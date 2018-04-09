package server.sql;

import java.util.*;
import java.util.function.Predicate;

public class SQLParser {
    public static class SQLAst {
        String action;
        String conditionCol;
        Double conditionComparator;
        Object conditionVal;
        Predicate<Map<String, Object>> conditionFunc = this::defaultAll;

        List<String> selectCols;
        List<String> joinSelectCols;

        boolean isJoin = false;

        Map<String, Object> newVal;
        Map<String, Class> schema;

        public String table;
        public String joinTable;

        String onConditionCol;
        String onConditionJoinCol;

        public boolean lt(Map<String, Object> obj) {
            return (Double) obj.get(conditionCol) < conditionComparator;
        }

        public boolean gt(Map<String, Object> obj) {
            return (Double) obj.get(conditionCol) > conditionComparator;
        }

        public boolean eq(Map<String, Object> obj) {
            return obj.get(conditionCol).equals(conditionVal);
        }

        public boolean defaultAll(Map<String, Object> obj) {
            return true;
        }
    }

    static List<SQLScanner.SQLTokenType> compareTokens = Arrays.asList(
            SQLScanner.SQLTokenType.GT,
            SQLScanner.SQLTokenType.LT,
            SQLScanner.SQLTokenType.EQ
    );

    /**
     * parse the token stream as ast
     *
     * @param tokens tokens from Scanner
     * @return Abstract syntax tree
     */
    public static SQLAst parse(List<SQLScanner.SQLToken> tokens) {
        SQLAst result = new SQLAst();
        switch (tokens.get(0).getType()) {
            case SELECT:
                if (tokens.size() < 4
                        || tokens.get(1).getType() != SQLScanner.SQLTokenType.VALUE
                        || tokens.get(2).getType() != SQLScanner.SQLTokenType.FROM
                        || tokens.get(3).getType() != SQLScanner.SQLTokenType.VALUE) {
                    throw new SQLException("Invalid select structure");
                }
                if (tokens.size() > 8) {
                    result.isJoin = true;
                    result.table = tokens.get(3).value;
                    int onStartIndex = 6;
                    // Hardcode for join
                    if (tokens.get(4).getType() == SQLScanner.SQLTokenType.JOIN) {
                        // NO where condition
                        result.joinTable = tokens.get(5).value;

                    }
                    else if (tokens.get(8).getType() == SQLScanner.SQLTokenType.JOIN){
                        result.joinTable = tokens.get(9).value;
                        parseWhere(tokens.subList(4, 8), result);
                        onStartIndex = 10;
                    }
                    else {
                        throw new SQLException("Invalid select structure");
                    }

                    parseJoinCols(tokens.get(1).getValue(), result);
                    parseJoinON(tokens.subList(onStartIndex, tokens.size()), result);
                }
                else {
                    result.selectCols = Arrays.asList(tokens.get(1).getValue().split(","));
                    result.table = tokens.get(3).value;
                    if (tokens.size() > 4) {
                        parseWhere(tokens.subList(4, tokens.size()), result);
                    }
                }
                break;
            case DELETE:
                if (tokens.size() < 3
                        || tokens.get(1).getType() != SQLScanner.SQLTokenType.FROM
                        || tokens.get(2).getType() != SQLScanner.SQLTokenType.VALUE) {
                    throw new SQLException("Invalid delete structure");
                }
                result.table = tokens.get(2).value;

                if (tokens.size() > 3) {
                    parseWhere(tokens.subList(3, tokens.size()), result);
                }
                break;
            case INSERT:
                if (tokens.size() != 4
                        || tokens.get(1).getType() != SQLScanner.SQLTokenType.VALUE
                        || tokens.get(2).getType() != SQLScanner.SQLTokenType.TO
                        || tokens.get(3).getType() != SQLScanner.SQLTokenType.VALUE) {
                    throw new SQLException("Invalid insert structure");
                }
                result.newVal = SQLIterateTable.jsonToMap(tokens.get(1).value);
                result.table = tokens.get(3).value;
                break;
            case UPDATE:
                if (tokens.size() < 4
                        || tokens.get(1).getType() != SQLScanner.SQLTokenType.VALUE
                        || tokens.get(2).getType() != SQLScanner.SQLTokenType.TO
                        || tokens.get(3).getType() != SQLScanner.SQLTokenType.VALUE) {
                    throw new SQLException("Invalid update structure");
                }
                result.newVal = SQLIterateTable.jsonToMap(tokens.get(1).value);
                result.table = tokens.get(3).value;

                if (tokens.size() > 4) {
                    parseWhere(tokens.subList(4, tokens.size()), result);
                }
                break;

            case DROP:
                if (tokens.size() != 2
                        || tokens.get(1).getType() != SQLScanner.SQLTokenType.VALUE) {
                    throw new SQLException("Invalid drop structure");
                }
                result.table = tokens.get(1).value;
                break;

            case CREATE:
                if (tokens.size() != 3
                        || tokens.get(1).getType() != SQLScanner.SQLTokenType.VALUE
                        || tokens.get(2).getType() != SQLScanner.SQLTokenType.VALUE) {
                    throw new SQLException("Invalid create structure");
                }
                result.table = tokens.get(1).value;
                result.schema = parseSchema(tokens.get(2).value);
                break;
            default:
                throw new SQLException("Invalid operation type");
        }
        result.action = tokens.get(0).getType().toString();
        return result;
    }

    private static String[] getColPair(String colString) {
        String[] result =  colString.split("\\.");
        if (result.length != 2) {
            throw new SQLException("invalid col format for join statement");
        }
        return result;
    }

    private static void parseJoinCols(String colString, SQLAst result){
        String[] cols = colString.split(",");
        result.selectCols = new ArrayList<>();
        result.joinSelectCols = new ArrayList<>();
        for (String col: cols) {
            String[] vals = getColPair(col);
            if (vals[0].equals(result.table)) {
                result.selectCols.add(vals[1]);
            } else if (vals[0].equals(result.joinTable)) {
                result.joinSelectCols.add(vals[1]);
            } else {
                throw new SQLException("invalid table name in 'TABLE_NAME.COLUMN_NAME'");
            }
        }
    }

    private static void parseJoinON(List<SQLScanner.SQLToken> tokens, SQLAst result) {
        if (tokens.size() != 4
                || tokens.get(0).getType() != SQLScanner.SQLTokenType.ON
                || tokens.get(2).getType() != SQLScanner.SQLTokenType.EQ){
            throw new SQLException("Invalid on structure");
        }
        String[] col1 = getColPair(tokens.get(1).getValue());
        String[] col2 = getColPair(tokens.get(3).getValue());
        if (col1[0].equals(result.table) && col2[0].equals(result.joinTable)) {
            result.onConditionCol = col1[1];
            result.onConditionJoinCol = col2[1];
        }
        else if (col1[0].equals(result.joinTable) && col2[0].equals(result.table)) {
            result.onConditionCol = col2[1];
            result.onConditionJoinCol = col1[1];
        }
        else {
            throw new SQLException("Invalid on condition: invalid table name");
        }
        if (!result.selectCols.contains(result.onConditionCol)){
            result.selectCols.add(result.onConditionCol);
        }

    }

    private static void parseWhere(List<SQLScanner.SQLToken> tokens, SQLAst result) {
        if (tokens.size() != 4
                || tokens.get(0).getType() != SQLScanner.SQLTokenType.WHERE
                || tokens.get(1).getType() != SQLScanner.SQLTokenType.VALUE
                || (!compareTokens.contains(tokens.get(2).getType()))
                || tokens.get(3).getType() != SQLScanner.SQLTokenType.VALUE) {
            throw new SQLException("Invalid where structure");
        }

        result.conditionCol = tokens.get(1).value;
        switch (tokens.get(2).getType()) {
            case GT:
                result.conditionFunc = result::gt;
                result.conditionComparator = Double.parseDouble(tokens.get(3).value);
                break;
            case LT:
                result.conditionFunc = result::lt;
                result.conditionComparator = Double.parseDouble(tokens.get(3).value);
            case EQ:
                result.conditionFunc = result::eq;
                result.conditionVal = tokens.get(3).value;
        }

    }

    private static Map<String, Class> parseSchema(String schema) {
        Map<String, Class> result = new HashMap<>();
        String[] cols = schema.split(",");
        for (String col : cols) {
            String[] strs = col.split(":");
            String field = strs[0];
            String val = strs[1];
            Class type;
            switch (val.toLowerCase()) {
                case "number":
                    type = Double.class;
                    break;
                case "string":
                    type = String.class;
                    break;
                default:
                    throw new SQLException("Invalid type for field " +
                            field + ", must be either Number or String");
            }
            result.put(field, type);
        }
        return result;
    }
}

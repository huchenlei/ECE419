package server.sql;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public class SQLParser {
    public static class SQLAst {
        String action;
        String conditionCol;
        Double conditionComparator;
        Object conditionVal;
        Predicate<Map<String, Object>> conditionFunc = this::defaultAll;

        List<String> selectCols;

        Map<String, Object> newVal;
        Map<String, Class> schema;

        public String table;

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
                result.selectCols = Arrays.asList(tokens.get(1).getValue().split(","));
                result.table = tokens.get(3).value;

                if (tokens.size() > 4) {
                    parseWhere(tokens.subList(4, tokens.size()), result);
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

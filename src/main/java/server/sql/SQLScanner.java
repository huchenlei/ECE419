package server.sql;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public class SQLScanner {
    public enum SQLTokenType {
        SELECT,
        TO,
        FROM,
        WHERE,
        INSERT,
        DELETE,
        UPDATE,

        DROP,
        CREATE,

        GT,
        LT,
        EQ,

        // actual values
        VALUE
    }

    public static class SQLToken {
        SQLTokenType type;
        String value = null;

        SQLToken(SQLTokenType type, String value) {
            this.type = type;
            this.value = value;
        }

        SQLToken(SQLTokenType type) {
            this.type = type;
        }

        public SQLTokenType getType() {
            return type;
        }

        public String getValue() {
            return value;
        }
    }

    private static HashSet valueTypes = new HashSet<SQLTokenType>() {
        {
            add(SQLTokenType.VALUE);
        }
    };

    /**
     * White space is not allowed in any value
     *
     * @param input input string from user
     * @return tokens
     */
    public static List<SQLToken> scan(String input) {
        return Arrays.stream(input.split("\\s")).map(s -> {
            SQLTokenType tokenType = SQLTokenType.VALUE;
            try {
                switch (s) {
                    case "<":
                        tokenType = SQLTokenType.LT;
                        break;
                    case ">":
                        tokenType = SQLTokenType.GT;
                        break;
                    case "=":
                        tokenType = SQLTokenType.EQ;
                        break;
                    default:
                        tokenType = SQLTokenType.valueOf(s.toUpperCase());
                }
                if (!valueTypes.contains(tokenType)) {
                    return new SQLToken(tokenType);
                }
            } catch (IllegalArgumentException iae) {
                // Do nothing
            }
            return new SQLToken(tokenType, s);
        }).collect(Collectors.toList());
    }
}

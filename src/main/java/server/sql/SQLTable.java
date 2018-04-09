package server.sql;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public interface SQLTable {
    /**
     * SQL query
     *
     * @param selector  cols to select
     * @param condition select all rows that meet the condition
     * @return cols selected in rows where condition is met
     * @throws SQLException exception
     */
    List<Map<String, Object>> query(List<String> selector,
                                    Predicate<Map<String, Object>> condition) throws SQLException, IOException;

    /**
     * SQL update
     *
     * @param newValue  new value to be updated to selected rows
     * @param condition select all rows that meet the condition
     * @return how many rows affected
     * @throws SQLException exception
     */
    Integer update(Map<String, Object> newValue,
                   Predicate<Map<String, Object>> condition) throws SQLException, IOException;

    /**
     * SQL insert
     * if no exception thrown, the insertion is successful
     *
     * @param value object to be inserted
     * @throws SQLException exception
     */
    void insert(Map<String, Object> value) throws SQLException, IOException;

    /**
     * SQL delete
     * delete all rows selected
     *
     * @param condition select all rows that meet the condition
     * @return how many rows affected
     * @throws SQLException exception
     */
    Integer delete(Predicate<Map<String, Object>> condition) throws SQLException, IOException;

    /**
     * SQL drop
     * Drop the whole table
     *
     * @throws IOException exception
     */
    void drop() throws IOException;

    /**
     * SQL joinSearch
     * @param colName
     * @param valueList
     * @param selector
     * @return
     * @throws SQLException
     * @throws IOException
     */
    Map<String, Map<String, Object>> joinSearch(String colName, List<Object> valueList, List<String> selector) throws SQLException, IOException;
}

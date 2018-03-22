package server.sql;

import java.util.Map;

public interface SQLPersistentStore {
    /**
     * Access tables by name
     *
     * @param name name of table
     * @return table obj
     * @throws SQLException exception
     */
    SQLTable getTableByName(String name) throws SQLException;

    /**
     * Create table SQL
     *
     * @param name name of the table
     * @param cols col specification of the table
     * @throws SQLException exception
     */
    void createTable(String name, Map<String, Class> cols) throws SQLException;

    /**
     * Drop table SQL
     *
     * @param name name of the table
     * @throws SQLException exception
     */
    void dropTable(String name) throws SQLException;
}

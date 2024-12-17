package de.betacoding.mysql.connection;

import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Abstraction for obtaining and managing database connections.
 */
public interface ConnectionProvider {

    /**
     * Retrieves a new or existing database connection.
     *
     * @return a {@link Connection} instance
     * @throws SQLException if a database access error occurs
     */
    @NotNull
    Connection getConnection() throws SQLException;

    /**
     * Closes the connection provider and releases any resources.
     *
     * @throws SQLException if a database access error occurs
     */
    void close() throws SQLException;
}
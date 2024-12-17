package de.betacoding.mysql.connection;

import de.betacoding.mysql.config.DatabaseConfig;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;

/**
 * A basic implementation of {@link ConnectionProvider} that creates a new
 * database connection for each request without pooling.
 */
public class BasicConnectionProvider implements ConnectionProvider {

    private final DatabaseConfig config;

    /**
     * Constructs a {@code BasicConnectionProvider} with the specified configuration.
     *
     * @param config the database configuration
     */
    public BasicConnectionProvider(@NotNull DatabaseConfig config) {
        this.config = Objects.requireNonNull(config, "DatabaseConfig cannot be null");
    }

    /**
     * Establishes and returns a new database connection using the provided configuration.
     *
     * @return a new {@link Connection} instance
     * @throws SQLException if a database access error occurs
     */
    @Override
    @NotNull
    public Connection getConnection() throws SQLException {
        String url = this.config.getConnectionUrl();
        String user = this.config.getUsername();
        String password = this.config.getPassword();
        return DriverManager.getConnection(url, user, password);
    }

    /**
     * Closes the connection provider. For {@code BasicConnectionProvider}, this method
     * does nothing as connections are not pooled and managed individually.
     *
     * @throws SQLException never thrown in this implementation
     */
    @Override
    public void close() throws SQLException {
        // No resources to close in BasicConnectionProvider
    }
}
package de.betacoding.mysql.connection;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import de.betacoding.mysql.config.DatabaseConfig;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;

/**
 * A connection provider implementation that uses HikariCP for connection pooling.
 */
public class PooledConnectionProvider implements ConnectionProvider {

    private final HikariDataSource dataSource;

    /**
     * Private constructor to enforce the use of the Builder.
     *
     * @param builder the Builder instance containing configuration settings
     */
    private PooledConnectionProvider(@NotNull Builder builder) {
        Objects.requireNonNull(builder, "Builder cannot be null");
        HikariConfig hikariConfig = new HikariConfig();

        // Set basic configurations from DatabaseConfig
        DatabaseConfig config = builder.databaseConfig;
        hikariConfig.setJdbcUrl(config.getConnectionUrl());
        hikariConfig.setUsername(config.getUsername());
        hikariConfig.setPassword(config.getPassword());

        // Set pool configurations from Builder
        hikariConfig.setPoolName(builder.poolName);
        hikariConfig.setMaximumPoolSize(builder.maximumPoolSize);
        hikariConfig.setMinimumIdle(builder.minimumIdle);
        hikariConfig.setIdleTimeout(builder.idleTimeout);
        hikariConfig.setConnectionTimeout(builder.connectionTimeout);

        // Apply any additional properties from DatabaseConfig
        if (!config.getAdditionalProperties().isEmpty()) {
            Properties additionalProps = config.getAdditionalProperties();
            additionalProps.forEach((key, value) -> {
                String keyStr = key.toString();
                String valueStr = value.toString();
                hikariConfig.addDataSourceProperty(keyStr, valueStr);
            });
        }

        // Apply any additional properties from Builder
        if (!builder.additionalProperties.isEmpty()) {
            builder.additionalProperties.forEach((key, value) -> {
                String keyStr = key.toString();
                String valueStr = value.toString();
                hikariConfig.addDataSourceProperty(keyStr, valueStr);
            });
        }

        this.dataSource = new HikariDataSource(hikariConfig);
    }

    /**
     * Retrieves a connection from the HikariCP pool.
     *
     * @return a {@link Connection} instance from the pool
     * @throws SQLException if unable to obtain a connection
     */
    @Override
    @NotNull
    public Connection getConnection() throws SQLException {
        return this.dataSource.getConnection();
    }

    /**
     * Closes the HikariCP data source and releases all pooled connections.
     *
     * @throws SQLException if a database access error occurs during shutdown
     */
    @Override
    public void close() throws SQLException {
        if (this.dataSource != null && !this.dataSource.isClosed()) {
            dataSource.close();
        }
    }

    /**
     * Optionally, provide access to the underlying HikariDataSource for advanced configurations or monitoring.
     *
     * @return the {@link HikariDataSource} instance
     */
    @Nullable
    public HikariDataSource getDataSource() {
        return this.dataSource;
    }

    /**
     * Builder class for constructing an instance of {@link PooledConnectionProvider} with custom HikariCP settings.
     */
    public static class Builder {
        private final DatabaseConfig databaseConfig;

        // HikariCP specific settings with default values
        private int maximumPoolSize = 10;
        private int minimumIdle = 2;
        private long idleTimeout = 300_000; // 5 minutes in milliseconds
        private long connectionTimeout = 10_000; // 10 seconds in milliseconds
        private String poolName;
        private final Properties additionalProperties = new Properties();

        /**
         * Constructs a Builder with the required {@link DatabaseConfig}.
         *
         * @param databaseConfig the database configuration
         */
        public Builder(@NotNull DatabaseConfig databaseConfig, @NotNull String poolName) {
            this.databaseConfig = Objects.requireNonNull(databaseConfig, "DatabaseConfig cannot be null");
            this.poolName = Objects.requireNonNull(poolName, "PoolName cannot be null");
        }

        /**
         * Sets the maximum number of connections in the pool.
         *
         * @param maximumPoolSize the maximum pool size
         * @return the Builder instance
         */
        public Builder setMaximumPoolSize(int maximumPoolSize) {
            if (maximumPoolSize <= 0) {
                throw new IllegalArgumentException("Maximum pool size must be positive");
            }
            this.maximumPoolSize = maximumPoolSize;
            return this;
        }

        /**
         * Sets the minimum number of idle connections in the pool.
         *
         * @param minimumIdle the minimum idle connections
         * @return the Builder instance
         */
        public Builder setMinimumIdle(int minimumIdle) {
            if (minimumIdle < 0) {
                throw new IllegalArgumentException("Minimum idle connections cannot be negative");
            }
            this.minimumIdle = minimumIdle;
            return this;
        }

        /**
         * Sets the idle timeout in milliseconds.
         *
         * @param idleTimeout the idle timeout
         * @return the Builder instance
         */
        public Builder setIdleTimeout(long idleTimeout) {
            if (idleTimeout < 0) {
                throw new IllegalArgumentException("Idle timeout cannot be negative");
            }
            this.idleTimeout = idleTimeout;
            return this;
        }

        /**
         * Sets the connection timeout in milliseconds.
         *
         * @param connectionTimeout the connection timeout
         * @return the Builder instance
         */
        public Builder setConnectionTimeout(long connectionTimeout) {
            if (connectionTimeout < 0) {
                throw new IllegalArgumentException("Connection timeout cannot be negative");
            }
            this.connectionTimeout = connectionTimeout;
            return this;
        }

        /**
         * Sets the name of the connection pool.
         *
         * @param poolName the pool name
         * @return the Builder instance
         */
        public Builder setPoolName(@NotNull String poolName) {
            this.poolName = Objects.requireNonNull(poolName, "Pool name cannot be null");
            return this;
        }

        /**
         * Adds an additional HikariCP property.
         *
         * @param key   the property key
         * @param value the property value
         * @return the Builder instance
         */
        public Builder addProperty(@NotNull String key, @NotNull String value) {
            Objects.requireNonNull(key, "Property key cannot be null");
            Objects.requireNonNull(value, "Property value cannot be null");
            this.additionalProperties.setProperty(key, value);
            return this;
        }

        /**
         * Builds and returns an instance of {@link PooledConnectionProvider} with the configured settings.
         *
         * @return the configured {@link PooledConnectionProvider} instance
         */
        @NotNull
        public PooledConnectionProvider build() {
            return new PooledConnectionProvider(this);
        }
    }
}
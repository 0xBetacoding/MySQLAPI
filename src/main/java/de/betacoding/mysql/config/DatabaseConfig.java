package de.betacoding.mysql.config;

import org.jetbrains.annotations.NotNull;

import java.util.Objects;
import java.util.Properties;

/**
 * Represents the configuration required to establish a connection to a MySQL/MariaDB database.
 * Implements the fluent builder pattern for easy and readable configuration setup.
 */
public class DatabaseConfig {

    private String host;
    private int port;
    private String databaseName;
    private String username;
    private String password;
    private Properties additionalProps;

    /**
     * Default constructor initializes additional properties.
     */
    public DatabaseConfig() {
        this.additionalProps = new Properties();
    }

    /**
     * Returns the host of the database server.
     *
     * @return the database host
     */
    @NotNull
    public String getHost() {
        return this.host;
    }

    /**
     * Sets the host of the database server.
     *
     * @param host the database host
     * @return the current DatabaseConfig instance
     */
    public DatabaseConfig setHost(@NotNull String host) {
        this.host = Objects.requireNonNull(host, "Host cannot be null");
        return this;
    }

    /**
     * Returns the port number of the database server.
     *
     * @return the database port
     */
    public int getPort() {
        return this.port;
    }

    /**
     * Sets the port number of the database server.
     *
     * @param port the database port
     * @return the current DatabaseConfig instance
     */
    public DatabaseConfig setPort(int port) {
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("Port must be between 1 and 65535");
        }
        this.port = port;
        return this;
    }

    /**
     * Returns the name of the database.
     *
     * @return the database name
     */
    @NotNull
    public String getDatabaseName() {
        return this.databaseName;
    }

    /**
     * Sets the name of the database.
     *
     * @param databaseName the database name
     * @return the current DatabaseConfig instance
     */
    public DatabaseConfig setDatabaseName(@NotNull String databaseName) {
        this.databaseName = Objects.requireNonNull(databaseName, "Database name cannot be null");
        return this;
    }

    /**
     * Returns the username for database authentication.
     *
     * @return the database username
     */
    @NotNull
    public String getUsername() {
        return this.username;
    }

    /**
     * Sets the username for database authentication.
     *
     * @param username the database username
     * @return the current DatabaseConfig instance
     */
    public DatabaseConfig setUsername(@NotNull String username) {
        this.username = Objects.requireNonNull(username, "Username cannot be null");
        return this;
    }

    /**
     * Returns the password for database authentication.
     *
     * @return the database password
     */
    @NotNull
    public String getPassword() {
        return this.password;
    }

    /**
     * Sets the password for database authentication.
     *
     * @param password the database password
     * @return the current DatabaseConfig instance
     */
    public DatabaseConfig setPassword(@NotNull String password) {
        this.password = Objects.requireNonNull(password, "Password cannot be null");
        return this;
    }

    /**
     * Returns the additional properties for advanced configurations.
     *
     * @return the additional properties
     */
    @NotNull
    public Properties getAdditionalProperties() {
        return this.additionalProps;
    }

    /**
     * Sets the additional properties for advanced configurations.
     *
     * @param additionalProps the additional properties
     * @return the current DatabaseConfig instance
     */
    public DatabaseConfig setAdditionalProperties(@NotNull Properties additionalProps) {
        this.additionalProps = Objects.requireNonNull(additionalProps, "Additional properties cannot be null");
        return this;
    }

    /**
     * Constructs and returns the JDBC connection URL based on the current configuration.
     *
     * @return the JDBC connection URL
     */
    @NotNull
    public String getConnectionUrl() {
        StringBuilder urlBuilder = new StringBuilder();
        urlBuilder.append("jdbc:mysql://")
                .append(this.host)
                .append(":")
                .append(this.port)
                .append("/")
                .append(this.databaseName);

        if (!this.additionalProps.isEmpty()) {
            urlBuilder.append("?");
            this.additionalProps.forEach((key, value) ->
                    urlBuilder.append(key).append("=").append(value).append("&"));
            // Remove the trailing '&'
            urlBuilder.setLength(urlBuilder.length() - 1);
        }

        return urlBuilder.toString();
    }

    @Override
    public String toString() {
        return "DatabaseConfig{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", databaseName='" + databaseName + '\'' +
                ", username='" + username + '\'' +
                ", additionalProps=" + additionalProps +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DatabaseConfig that = (DatabaseConfig) o;

        if (this.port != that.port) return false;
        if (!this.host.equals(that.host)) return false;
        if (!this.databaseName.equals(that.databaseName)) return false;
        if (!this.username.equals(that.username)) return false;
        if (!this.password.equals(that.password)) return false;
        return this.additionalProps.equals(that.additionalProps);
    }

    @Override
    public int hashCode() {
        int result = this.host.hashCode();
        result = 31 * result + this.port;
        result = 31 * result + this.databaseName.hashCode();
        result = 31 * result + this.username.hashCode();
        result = 31 * result + this.password.hashCode();
        result = 31 * result + this.additionalProps.hashCode();
        return result;
    }
}

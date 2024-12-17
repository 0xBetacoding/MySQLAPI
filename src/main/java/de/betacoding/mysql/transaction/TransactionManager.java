package de.betacoding.mysql.transaction;

import de.betacoding.mysql.connection.ConnectionProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

/**
 * Manages database transactions, allowing you to begin, commit, and rollback transactions.
 * Utilizes ThreadLocal to ensure thread safety in multi-threaded environments.
 */
public class TransactionManager {

    private final ConnectionProvider connectionProvider;

    /**
     * ThreadLocal to store the current connection associated with the transaction for each thread.
     */
    private final ThreadLocal<Connection> currentConnection = new ThreadLocal<>();

    /**
     * Constructs a {@code TransactionManager} with the specified {@link ConnectionProvider}.
     *
     * @param connectionProvider the connection provider to obtain connections from
     */
    public TransactionManager(@NotNull ConnectionProvider connectionProvider) {
        this.connectionProvider = Objects.requireNonNull(connectionProvider, "ConnectionProvider cannot be null");
    }

    /**
     * Begins a new transaction. Sets auto-commit to false and stores the connection in ThreadLocal.
     *
     * @throws SQLException if a database access error occurs or a transaction is already active
     */
    public void begin() throws SQLException {
        if (this.currentConnection.get() != null) {
            throw new SQLException("A transaction is already active on this thread.");
        }
        Connection connection = this.connectionProvider.getConnection();
        connection.setAutoCommit(false);
        this.currentConnection.set(connection);
    }

    /**
     * Commits the current transaction and closes the connection. Restores auto-commit to true.
     *
     * @throws SQLException if a database access error occurs or no transaction is active
     */
    public void commit() throws SQLException {
        Connection connection = this.currentConnection.get();
        if (connection == null) {
            throw new SQLException("No active transaction to commit.");
        }
        try {
            connection.commit();
        } catch (SQLException e) {
            connection.rollback();
            throw new SQLException("Failed to commit transaction. Transaction has been rolled back.", e);
        } finally {
            try (connection) {
                connection.setAutoCommit(true);
            } finally {
                this.currentConnection.remove();
            }
        }
    }

    /**
     * Rolls back the current transaction and closes the connection. Restores auto-commit to true.
     *
     * @throws SQLException if a database access error occurs or no transaction is active
     */
    public void rollback() throws SQLException {
        Connection connection = this.currentConnection.get();
        if (connection == null) {
            throw new SQLException("No active transaction to rollback.");
        }
        try {
            connection.rollback();
        } finally {
            try (connection) {
                connection.setAutoCommit(true);
            } finally {
                this.currentConnection.remove();
            }
        }
    }

    /**
     * Retrieves the current connection associated with the active transaction.
     *
     * @return the current {@link Connection} if a transaction is active, otherwise {@code null}
     */
    @Nullable
    public Connection getCurrentConnection() {
        return this.currentConnection.get();
    }

    /**
     * Checks whether a transaction is currently active on the calling thread.
     *
     * @return {@code true} if a transaction is active, {@code false} otherwise
     */
    public boolean isTransactionActive() {
        return this.currentConnection.get() != null;
    }
}
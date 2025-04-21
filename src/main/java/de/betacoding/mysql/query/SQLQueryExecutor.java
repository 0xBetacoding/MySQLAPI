package de.betacoding.mysql.query;

import de.betacoding.mysql.connection.ConnectionProvider;
import de.betacoding.mysql.mapping.RowMapper;
import de.betacoding.mysql.transaction.SQLTransactionManager;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Facilitates the execution of SQL queries and updates using a {@link ConnectionProvider}.
 */
public class SQLQueryExecutor {

    private final ConnectionProvider connectionProvider;
    private final SQLTransactionManager transactionManager;

    /**
     * Constructs a {@code QueryExecutor} with the specified {@link ConnectionProvider} and {@link SQLTransactionManager}.
     *
     * @param connectionProvider the connection provider
     * @param transactionManager the transaction manager
     */
    public SQLQueryExecutor(@NotNull ConnectionProvider connectionProvider, @NotNull SQLTransactionManager transactionManager) {
        this.connectionProvider = connectionProvider;
        this.transactionManager = transactionManager;
    }

    /**
     * Retrieves the connection to be used for executing queries.
     * If a transaction is active, it returns the transaction's connection.
     * Otherwise, it obtains a new connection from the provider.
     *
     * @return a {@link Connection} instance
     * @throws SQLException if a database access error occurs
     */
    @NotNull
    private Connection getConnection() throws SQLException {
        Connection connection = this.transactionManager.getCurrentConnection();
        if (connection != null) {
            return connection;
        }
        return this.connectionProvider.getConnection();
    }

    /**
     * Executes a SELECT query and returns the {@link ResultSet}.
     *
     * @param sql    the SQL query to execute
     * @param params the parameters for the prepared statement
     * @return the result set of the query
     * @throws SQLException if a database access error occurs
     */
    public ResultSet executeQuery(@NotNull String sql, @Nullable Object... params) throws SQLException {
        Connection connection = this.getConnection();
        PreparedStatement statement = null;
        boolean isInTransaction = this.transactionManager.isTransactionActive();

        try {
            statement = connection.prepareStatement(sql);
            setParameters(statement, params);
            return statement.executeQuery();
        } finally {
            if (statement != null && !statement.isClosed()) {
                statement.close();
            }
            if (!isInTransaction) {
                if (!connection.isClosed()) {
                    connection.close();
                }
            }
            // In a transaction, do not close the connection; it will be managed by TransactionManager
        }
    }

    /**
     * Executes an INSERT, UPDATE, or DELETE statement and returns the number of affected rows.
     *
     * @param sql    the SQL statement to execute
     * @param params the parameters for the prepared statement
     * @return the number of rows affected
     * @throws SQLException if a database access error occurs
     */
    public int executeUpdate(@NotNull String sql,
                             @Nullable Object... params) throws SQLException {
        Connection connection = this.getConnection();
        PreparedStatement statement = null;
        boolean isInTransaction = this.transactionManager.isTransactionActive();

        try {
            statement = connection.prepareStatement(sql);
            setParameters(statement, params);
            return statement.executeUpdate();
        } finally {
            if (statement != null && !statement.isClosed()) {
                statement.close();
            }
            if (!isInTransaction) {
                if (!connection.isClosed()) {
                    connection.close();
                }
            }
            // In a transaction, do not close the connection; it will be managed by TransactionManager
        }
    }
    /**
     * Executes an INSERT statement and retrieves the generated key.
     *
     * @param sql    the INSERT SQL statement to execute
     * @param params the parameters for the prepared statement
     * @return an {@link Optional} containing the generated key if present, otherwise empty
     * @throws SQLException if a database access error occurs
     */
    public Optional<Long> executeInsert(@NotNull String sql,
                                        @Nullable Object... params) throws SQLException {
        Connection connection = this.getConnection();
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        boolean isInTransaction = this.transactionManager.isTransactionActive();

        try {
            statement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            setParameters(statement, params);
            int affectedRows = statement.executeUpdate();

            if (affectedRows == 0) {
                throw new SQLException("Executing insert failed, no rows affected.");
            }

            resultSet = statement.getGeneratedKeys();
            if (resultSet.next()) {
                long generatedKey = resultSet.getLong(1);
                return Optional.of(generatedKey);
            } else {
                return Optional.empty();
            }
        } finally {
            if (resultSet != null && !resultSet.isClosed()) {
                resultSet.close();
            }
            if (statement != null && !statement.isClosed()) {
                statement.close();
            }
            if (!isInTransaction) {
                if (!connection.isClosed()) {
                    connection.close();
                }
            }
            // In a transaction, do not close the connection; it will be managed by TransactionManager
        }
    }

    /**
     * Executes a SELECT query and maps the first result to an object of type T using the provided {@link RowMapper}.
     *
     * @param sql    the SQL query to execute
     * @param mapper the row mapper to convert ResultSet rows to objects
     * @param params the parameters for the prepared statement
     * @param <T>    the type of the result object
     * @return an {@link Optional} containing the mapped object if present, otherwise empty
     * @throws SQLException if a database access error occurs
     */
    public <T> Optional<T> queryForObject(@NotNull String sql,
                                          @NotNull RowMapper<T> mapper,
                                          @Nullable Object... params) throws SQLException {
        Connection connection = this.getConnection();
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        boolean isInTransaction = this.transactionManager.isTransactionActive();

        try {
            statement = connection.prepareStatement(sql);
            setParameters(statement, params);
            resultSet = statement.executeQuery();
            if (resultSet.next()) {
                T obj = mapper.mapRow(resultSet);
                return Optional.ofNullable(obj);
            }
            return Optional.empty();
        } finally {
            if (resultSet != null && !resultSet.isClosed()) {
                resultSet.close();
            }
            if (statement != null && !statement.isClosed()) {
                statement.close();
            }
            if (!isInTransaction) {
                if (!connection.isClosed()) {
                    connection.close();
                }
            }
            // In a transaction, do not close the connection; it will be managed by TransactionManager
        }
    }

    /**
     * Executes a SELECT query and maps the results to a list of objects of type T using the provided {@link RowMapper}.
     *
     * @param sql    the SQL query to execute
     * @param mapper the row mapper to convert ResultSet rows to objects
     * @param params the parameters for the prepared statement
     * @param <T>    the type of the result objects
     * @return a list of mapped objects
     * @throws SQLException if a database access error occurs
     */
    public <T> List<T> queryForList(@NotNull String sql,
                                    @NotNull RowMapper<T> mapper,
                                    @Nullable Object... params) throws SQLException {
        List<T> results = new ArrayList<>();
        Connection connection = this.getConnection();
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        boolean isInTransaction = this.transactionManager.isTransactionActive();

        try {
            statement = connection.prepareStatement(sql);
            setParameters(statement, params);
            resultSet = statement.executeQuery();
            while (resultSet.next()) {
                T obj = mapper.mapRow(resultSet);
                results.add(obj);
            }
        } finally {
            if (resultSet != null && !resultSet.isClosed()) {
                resultSet.close();
            }
            if (statement != null && !statement.isClosed()) {
                statement.close();
            }
            if (!isInTransaction) {
                if (!connection.isClosed()) {
                    connection.close();
                }
            }
            // In a transaction, do not close the connection; it will be managed by TransactionManager
        }
        return results;
    }

    /**
     * Executes a batch update with the specified SQL and batch parameters.
     *
     * @param sql         the SQL statement to execute
     * @param batchParams a list of parameter arrays for each batch
     * @return an array of update counts containing one element for each command in the batch
     * @throws SQLException if a database access error occurs
     */
    public int[] batchUpdate(@NotNull String sql,
                             @NotNull List<Object[]> batchParams) throws SQLException {
        if (batchParams.isEmpty()) {
            return new int[0];
        }

        Connection connection = this.getConnection();
        PreparedStatement statement = null;
        boolean isInTransaction = this.transactionManager.isTransactionActive();

        try {
            statement = connection.prepareStatement(sql);
            for (Object[] params : batchParams) {
                setParameters(statement, params);
                statement.addBatch();
            }
            return statement.executeBatch();
        } finally {
            if (statement != null && !statement.isClosed()) {
                statement.close();
            }
            if (!isInTransaction) {
                if (!connection.isClosed()) {
                    connection.close();
                }
            }
            // In a transaction, do not close the connection; it will be managed by TransactionManager
        }
    }

    /**
     * Executes a query using a {@link PreparedStatementCreator} and maps the first result to an object of type T.
     *
     * @param creator the prepared statement creator
     * @param mapper  the row mapper to convert ResultSet rows to objects
     * @param <T>     the type of the result object
     * @return an {@link Optional} containing the mapped object if present, otherwise empty
     * @throws SQLException if a database access error occurs
     */
    public <T> Optional<T> queryForObject(@NotNull PreparedStatementCreator creator,
                                          @NotNull RowMapper<T> mapper) throws SQLException {
        String sql = creator.getSql();
        Object[] params = creator.getParameters();
        return queryForObject(sql, mapper, params);
    }

    /**
     * Executes a query using a {@link PreparedStatementCreator} and maps the results to a list of objects of type T.
     *
     * @param creator the prepared statement creator
     * @param mapper  the row mapper to convert ResultSet rows to objects
     * @param <T>     the type of the result objects
     * @return a list of mapped objects
     * @throws SQLException if a database access error occurs
     */
    public <T> List<T> queryForList(@NotNull PreparedStatementCreator creator,
                                    @NotNull RowMapper<T> mapper) throws SQLException {
        String sql = creator.getSql();
        Object[] params = creator.getParameters();
        return queryForList(sql, mapper, params);
    }

    /**
     * Executes an update using a {@link PreparedStatementCreator}.
     *
     * @param creator the prepared statement creator
     * @return the number of rows affected
     * @throws SQLException if a database access error occurs
     */
    public int executeUpdate(@NotNull PreparedStatementCreator creator) throws SQLException {
        String sql = creator.getSql();
        Object[] params = creator.getParameters();
        return executeUpdate(sql, params);
    }

    /**
     * Executes a batch update using a {@link PreparedStatementCreator} and a list of batch parameters.
     *
     * @param creator     the prepared statement creator
     * @param batchParams a list of parameter arrays for each batch
     * @return an array of update counts containing one element for each command in the batch
     * @throws SQLException if a database access error occurs
     */
    public int[] batchUpdate(@NotNull PreparedStatementCreator creator,
                             @NotNull List<Object[]> batchParams) throws SQLException {
        String sql = creator.getSql();
        return batchUpdate(sql, batchParams);
    }

    /**
     * Sets the parameters of the {@link PreparedStatement}.
     *
     * @param statement the prepared statement
     * @param params    the parameters to set
     * @throws SQLException if a database access error occurs
     */
    private void setParameters(@NotNull PreparedStatement statement, @Nullable Object... params) throws SQLException {
        if (params != null) {
            for (int i = 0; i < params.length; i++) {
                statement.setObject(i + 1, params[i]);
            }
        }
    }
}
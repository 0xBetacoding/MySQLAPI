package de.betacoding.mysql.query;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Objects;

/**
 * Default implementation of {@link PreparedStatementCreator} that sets parameters
 * in the order they are provided.
 */
public class DefaultPreparedStatementCreator implements PreparedStatementCreator {

    private final String sql;
    private final Object[] parameters;

    /**
     * Constructs a {@code DefaultPreparedStatementCreator} with the specified SQL and parameters.
     *
     * @param sql        the SQL statement
     * @param parameters the parameters to set in the prepared statement
     */
    public DefaultPreparedStatementCreator(@NotNull String sql, @Nullable Object... parameters) {
        this.sql = Objects.requireNonNull(sql, "SQL cannot be null");
        this.parameters = parameters;
    }

    /**
     * Creates a {@link PreparedStatement} and sets the provided parameters.
     *
     * @param connection the database connection
     * @return a prepared statement with parameters set
     * @throws SQLException if a database access error occurs
     */
    @Override
    @NotNull
    public PreparedStatement createPreparedStatement(@NotNull Connection connection) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement(this.sql);
        if (this.parameters != null) {
            for (int i = 0; i < this.parameters.length; i++) {
                preparedStatement.setObject(i + 1, this.parameters[i]);
            }
        }
        return preparedStatement;
    }

    /**
     * Returns the SQL statement associated with this prepared statement creator.
     *
     * @return the SQL string
     */
    @Override
    @NotNull
    public String getSql() {
        return this.sql;
    }

    @Override
    @Nullable
    public Object[] getParameters() {
        return this.parameters;
    }
}
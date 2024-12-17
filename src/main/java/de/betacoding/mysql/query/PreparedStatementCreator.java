package de.betacoding.mysql.query;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Interface for creating {@link PreparedStatement} instances with specific SQL and parameters.
 */
public interface PreparedStatementCreator {

    /**
     * Creates a {@link PreparedStatement} using the provided {@link Connection}.
     *
     * @param connection the database connection
     * @return a prepared statement
     * @throws SQLException if a database access error occurs
     */
    @NotNull
    PreparedStatement createPreparedStatement(@NotNull Connection connection) throws SQLException;

    /**
     * Provides the SQL statement associated with this prepared statement creator.
     *
     * @return the SQL string
     */
    @NotNull
    String getSql();

    @Nullable
    Object[] getParameters();
}
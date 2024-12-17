package de.betacoding.mysql.mapping;

import org.jetbrains.annotations.NotNull;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Interface for mapping a row of a {@link ResultSet} to a Java object.
 *
 * @param <T> the type of the mapped object
 */
public interface RowMapper<T> {

    /**
     * Maps the current row of the given {@link ResultSet} to an object of type T.
     *
     * @param rs the result set positioned at the current row
     * @return the mapped object
     * @throws SQLException if a database access error occurs
     */
    @NotNull
    T mapRow(@NotNull ResultSet rs) throws SQLException;
}
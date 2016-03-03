package com.github.hoesler.dbj;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface StatementUpdater {
    /**
     * Inject the value of a column at {@code columnIndex} into a prepared statement at {@code statementIndex}.
     * @param statement the statement to inject into
     * @param statementIndex the index at which to inject
     * @param columnIndex the index of the value in the column
     * @throws SQLException
     */
    void updateStatement(PreparedStatement statement, int statementIndex, int columnIndex) throws SQLException;
}

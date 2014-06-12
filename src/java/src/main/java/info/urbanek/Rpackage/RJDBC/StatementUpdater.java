package info.urbanek.Rpackage.RJDBC;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface StatementUpdater {
    void updateStatement(PreparedStatement statement, int statementIndex, int columnIndex) throws SQLException;
}

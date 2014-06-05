package info.urbanek.Rpackage.RJDBC;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public interface Column<T> extends List<T> {

    ColumnType getColumnType();

    void addFromResultSet(ResultSet resultSet, final int i) throws SQLException;

    void update(PreparedStatement statement, int statementIndex, final int columnIndex) throws SQLException;

    boolean[] getNA();
}

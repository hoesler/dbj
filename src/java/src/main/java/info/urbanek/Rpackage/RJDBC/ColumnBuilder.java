package info.urbanek.Rpackage.RJDBC;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface ColumnBuilder<T extends Column<?>> {

    T build();

    void clear();

    void addFromResultSet(ResultSet resultSet, int i) throws SQLException;
}

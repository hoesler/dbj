package info.urbanek.Rpackage.RJDBC;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface Column<T> {

    void add(final T value);

    void set(final int index, final T value);

    ColumnType getColumnType();

    Object[] toObjectArray();

    String[] toStringArray();

    double[] toDoubleArray();

    void clear();

    int size();

    void addFromResultSet(ResultSet resultSet, final int i) throws SQLException;
}

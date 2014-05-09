package info.urbanek.Rpackage.RJDBC;

import java.sql.ResultSet;
import java.sql.SQLException;

final class StringColumn extends AbstractColumn<String> {

    public ColumnType getColumnType() {
        return ColumnType.NUMERIC;
    }

    public Object[] toObjectArray() {
        return data.toArray();
    }

    public String[] toStringArray() {
        return data.toArray(new String[data.size()]);
    }

    public double[] toDoubleArray() {
        throw new UnsupportedOperationException("This is a String column");
    }

    public void addFromResultSet(final ResultSet resultSet, final int i) throws SQLException {
        add(resultSet.getString(i));
    }
}

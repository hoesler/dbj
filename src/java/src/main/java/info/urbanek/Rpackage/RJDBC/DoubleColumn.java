package info.urbanek.Rpackage.RJDBC;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

final class DoubleColumn extends AbstractColumn<Double> {

    public ColumnType getColumnType() {
        return ColumnType.NUMERIC;
    }

    public Object[] toObjectArray() {
        return data.toArray();
    }

    public String[] toStringArray() {
        throw new UnsupportedOperationException("This is a numeric column");
    }

    public double[] toDoubleArray() {
        return toArray(data);
    }

    public void addFromResultSet(final ResultSet resultSet, final int i) throws SQLException {
        add(resultSet.getDouble(i));
    }

    public static double[] toArray(final Collection<? extends Number> collection) {
        Object[] boxedArray = collection.toArray();
        int len = boxedArray.length;
        double[] array = new double[len];
        for (int i = 0; i < len; i++) {
            // checkNotNull for GWT (do not optimize)
            array[i] = ((Number) boxedArray[i]).doubleValue();
        }
        return array;
    }
}

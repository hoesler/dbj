package info.urbanek.Rpackage.RJDBC;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.primitives.Booleans;

import java.sql.*;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public final class DoubleColumn extends AbstractList<Double> implements Column<Double> {

    private final static Double NA = Double.NaN;
    private final List<Optional<Double>> data;

    DoubleColumn() {
        data = new ArrayList<Optional<Double>>();
    }

    DoubleColumn(final Collection<Optional<Double>> values) {
        data = new ArrayList<Optional<Double>>(values);
    }

    @Override
    public int size() {
        return data.size();
    }

    @Override
    public Double get(final int i) {
        return data.get(i).get();
    }

    public ColumnType getColumnType() {
        return ColumnType.NUMERIC;
    }

    public double[] toDoubleArray() {
        final double[] doubles = new double[size()];
        for (int i = 0; i < data.size(); i++) {
            doubles[i] = data.get(i).or(NA);
        }
        return doubles;
    }

    public void addFromResultSet(final ResultSet resultSet, final int i) throws SQLException {
        final double aDouble = resultSet.getDouble(i);
        if (resultSet.wasNull()) {
            data.add(Optional.<Double>absent());
        } else {
            data.add(Optional.of(aDouble));
        }
    }

    @Override
    public void update(final PreparedStatement statement, final int statementIndex, final int columnIndex) throws SQLException {
        final Optional<Double> aDouble = data.get(columnIndex);
        if (!aDouble.isPresent()) {
            statement.setNull(statementIndex, Types.DOUBLE);
        } else {
            statement.setDouble(statementIndex, aDouble.get());
        }
    }

    @Override
    public boolean[] getNA() {
        return Booleans.toArray(Lists.transform(data, new Function<Optional<Double>, Boolean>() {
            @Override
            public Boolean apply(final Optional<Double> dateOptional) {
                return !dateOptional.isPresent();
            }
        }));
    }

    /**
     * Create a new DoubleColumn from an array of double values.
     *
     * @param doubles double values
     * @param na NA indicators
     * @return a new Date column
     */
    public static DoubleColumn create(double[] doubles, boolean[] na) {
        checkNotNull(doubles);
        checkNotNull(na);
        checkArgument(doubles.length == na.length);

        final ArrayList<Optional<Double>> optionals = new ArrayList<Optional<Double>>(doubles.length);
        for (int i = 0; i < doubles.length; i++) {
            optionals.add(na[i] ? Optional.<Double>absent() : Optional.of(doubles[i]));
        }
        return new DoubleColumn(optionals);
    }
}

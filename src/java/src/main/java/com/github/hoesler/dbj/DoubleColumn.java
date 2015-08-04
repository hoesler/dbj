package com.github.hoesler.dbj;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ForwardingList;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Booleans;
import com.google.common.primitives.Doubles;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public final class DoubleColumn extends ForwardingList<Optional<Double>> implements Column<Optional<Double>> {

    private final static Double NA = Double.NaN;
    private final List<Optional<Double>> data;
    private final int sqlType;
    private final StatementUpdater updater;

    DoubleColumn(final int sqlType, final Collection<Optional<Double>> values) {
        this.sqlType = sqlType;
        this.data = ImmutableList.copyOf(values);
        this.updater = createUpdater(sqlType);
    }

    private StatementUpdater createUpdater(final int sqlType) {
        if (sqlType == Types.DOUBLE) {
            return new DoubleUpdater();
        } else if (sqlType == Types.FLOAT || sqlType == Types.REAL) {
            return new FloatUpdater();
        } else if (sqlType == Types.NUMERIC || sqlType == Types.DECIMAL) {
            return new NumericUpdater();
        } else {
            throw new IllegalArgumentException("SQL type not supported by this column");
        }
    }

    @Override
    public int size() {
        return data.size();
    }

    @Override
    protected List<Optional<Double>> delegate() {
        return data;
    }

    @Override
    public Optional<Double> get(final int i) {
        return data.get(i);
    }

    public double[] toDoubles() {
        final double[] doubles = new double[size()];
        for (int i = 0; i < data.size(); i++) {
            doubles[i] = data.get(i).or(NA);
        }
        return doubles;
    }

    @Override
    public void updateStatement(final PreparedStatement statement, final int statementIndex, final int columnIndex) throws SQLException {
        updater.updateStatement(statement, statementIndex, columnIndex);
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

    @Override
    public int sqlType() {
        return sqlType;
    }

    @Override
    public String getSimpleClassName() {
        return getClass().getSimpleName();
    }

    public static DoubleColumn create(final int sqlType, double[] doubles) {
        checkNotNull(doubles);
        checkArgument(handles(sqlType));

        return new DoubleColumn(sqlType, Lists.transform(Doubles.asList(doubles),
                new Function<Double, Optional<Double>>() {
                    @Override
                    public Optional<Double> apply(final Double aDouble) {
                        return Optional.of(aDouble);
                    }
                }));
    }

    /**
     * Create a new DoubleColumn from an array of double values.
     *
     * @param sqlType the {@link java.sql.Types sql type} for this column
     * @param doubles double values
     * @param na      NA indicators
     * @return a new Date column
     */
    public static DoubleColumn create(final int sqlType, double[] doubles, boolean[] na) {
        checkNotNull(doubles);
        checkNotNull(na);
        checkArgument(doubles.length == na.length);
        checkArgument(handles(sqlType));

        final ArrayList<Optional<Double>> optionals = new ArrayList<Optional<Double>>(doubles.length);
        for (int i = 0; i < doubles.length; i++) {
            optionals.add(na[i] ? Optional.<Double>absent() : Optional.of(doubles[i]));
        }
        return new DoubleColumn(sqlType, optionals);
    }

    public static ColumnBuilder<DoubleColumn> builder(final int columnType) {
        return new DoubleColumnBuilder(columnType);
    }

    public static boolean handles(final int sqlType) {
        switch (sqlType) {
            case Types.FLOAT:
            case Types.REAL:
            case Types.NUMERIC:
            case Types.DECIMAL:
            case Types.DOUBLE:
                return true;
            default:
                return false;
        }
    }

    private static class DoubleColumnBuilder implements ColumnBuilder<DoubleColumn> {
        private final List<Optional<Double>> data = Lists.newArrayList();
        private final int columnType;
        private final ResultSetTransfer<Optional<Double>> transfer;

        public DoubleColumnBuilder(final int columnType) {
            this.columnType = columnType;
            transfer = createTransfer(columnType);
        }

        private ResultSetTransfer<Optional<Double>> createTransfer(final int columnType) {
            if (columnType == Types.DOUBLE) {
                return new DoubleResultSetTransfer();
            } else if (columnType == Types.FLOAT || columnType == Types.REAL) {
                return new FloatResultSetTransfer();
            } else if (columnType == Types.NUMERIC || columnType == Types.DECIMAL) {
                return new BigDecimalResultSetTransfer();
            } else {
                throw new IllegalArgumentException("Cannot create a result set transfer to double for sql type " + columnType);
            }
        }

        @Override
        public void addFromResultSet(final ResultSet resultSet, final int i) throws SQLException {
            transfer.addFromResultSet(data, resultSet, i);
        }

        @Override
        public DoubleColumn build() {
            return new DoubleColumn(columnType, data);
        }

        @Override
        public void clear() {
            data.clear();
        }

        private static class DoubleResultSetTransfer implements ResultSetTransfer<Optional<Double>> {
            @Override
            public void addFromResultSet(final Collection<? super Optional<Double>> collection, final ResultSet resultSet, final int i) throws SQLException {
                final double doubleValue = resultSet.getDouble(i);
                if (resultSet.wasNull()) {
                    collection.add(Optional.<Double>absent());
                } else {
                    collection.add(Optional.of(doubleValue));
                }
            }
        }

        private static class FloatResultSetTransfer implements ResultSetTransfer<Optional<Double>> {
            @Override
            public void addFromResultSet(final Collection<? super Optional<Double>> collection, final ResultSet resultSet, final int i) throws SQLException {
                final float floatValue = resultSet.getFloat(i);
                if (resultSet.wasNull()) {
                    collection.add(Optional.<Double>absent());
                } else {
                    collection.add(Optional.of((double) floatValue));
                }
            }
        }

        private static class BigDecimalResultSetTransfer implements ResultSetTransfer<Optional<Double>> {
            @Override
            public void addFromResultSet(final Collection<? super Optional<Double>> collection, final ResultSet resultSet, final int i) throws SQLException {
                final BigDecimal bigDecimalValue = resultSet.getBigDecimal(i);
                if (resultSet.wasNull()) {
                    collection.add(Optional.<Double>absent());
                } else {
                    collection.add(Optional.of(bigDecimalValue.doubleValue()));
                }
            }
        }
    }

    private class DoubleUpdater implements StatementUpdater {

        @Override
        public void updateStatement(final PreparedStatement statement, final int statementIndex, final int columnIndex) throws SQLException {
            final Optional<Double> aDouble = data.get(columnIndex);
            if (!aDouble.isPresent()) {
                statement.setNull(statementIndex, sqlType);
            } else {
                statement.setDouble(statementIndex, aDouble.get());
            }
        }
    }

    private class FloatUpdater implements StatementUpdater {

        @Override
        public void updateStatement(final PreparedStatement statement, final int statementIndex, final int columnIndex) throws SQLException {
            final Optional<Double> aDouble = data.get(columnIndex);
            if (!aDouble.isPresent()) {
                statement.setNull(statementIndex, sqlType);
            } else {
                statement.setFloat(statementIndex, aDouble.get().floatValue());
            }
        }
    }

    private class NumericUpdater implements StatementUpdater {

        @Override
        public void updateStatement(final PreparedStatement statement, final int statementIndex, final int columnIndex) throws SQLException {
            final Optional<Double> aDouble = data.get(columnIndex);
            if (!aDouble.isPresent()) {
                statement.setNull(statementIndex, sqlType);
            } else {
                statement.setBigDecimal(statementIndex, BigDecimal.valueOf(aDouble.get()));
            }
        }
    }
}

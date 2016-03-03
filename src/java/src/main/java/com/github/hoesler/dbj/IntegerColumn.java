package com.github.hoesler.dbj;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ForwardingList;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public final class IntegerColumn extends ForwardingList<Optional<Integer>> implements Column<Optional<Integer>> {

    private final static Integer NA = Integer.MIN_VALUE;
    private final List<Optional<Integer>> data;
    private final int sqlType;

    private IntegerColumn(final int sqlType, final Collection<Optional<Integer>> values) {
        this.sqlType = sqlType;
        data = ImmutableList.copyOf(values);
    }

    @Override
    public int size() {
        return data.size();
    }

    @Override
    protected List<Optional<Integer>> delegate() {
        return data;
    }

    @Override
    public Optional<Integer> get(final int i) {
        return data.get(i);
    }

    public int[] toInts() {
        final int[] ints = new int[size()];
        for (int i = 0; i < data.size(); i++) {
            ints[i] = data.get(i).or(NA);
        }
        return ints;
    }

    @Override
    public void updateStatement(final PreparedStatement statement, final int statementIndex, final int columnIndex) throws SQLException {
        final Optional<Integer> cast = data.get(columnIndex);
        if (!cast.isPresent()) {
            statement.setNull(statementIndex, sqlType);
        } else {
            statement.setInt(statementIndex, cast.get());
        }
    }

    @Override
    public boolean[] getNA() {
        return Columns.getOptionalStates(this);
    }

    @Override
    public int sqlType() {
        return sqlType;
    }

    @Override
    public String getSimpleClassName() {
        return getClass().getSimpleName();
    }

    public static IntegerColumn create(final int sqlType, int[] integers) {
        checkNotNull(integers);
        checkArgument(handles(sqlType));

        return new IntegerColumn(sqlType, Lists.transform(Ints.asList(integers),
                new Function<Integer, Optional<Integer>>() {
                    @Override
                    public Optional<Integer> apply(final Integer aInteger) {
                        return Optional.of(aInteger);
                    }
                }));
    }

    /**
     * Create a new IntegerColumn from an array of integer values.
     *
     * @param sqlType  the {@link java.sql.Types sql type} for this column
     * @param integers integer values
     * @param na       NA indicators
     * @return a new Date column
     */
    public static IntegerColumn create(final int sqlType, int[] integers, boolean[] na) {
        checkNotNull(integers);
        checkNotNull(na);
        checkArgument(integers.length == na.length);
        checkArgument(handles(sqlType));

        final ArrayList<Optional<Integer>> optionals = new ArrayList<Optional<Integer>>(integers.length);
        for (int i = 0; i < integers.length; i++) {
            optionals.add(na[i] ? Optional.<Integer>absent() : Optional.of(integers[i]));
        }
        return new IntegerColumn(sqlType, optionals);
    }

    public static ColumnBuilder<IntegerColumn> builder(final int sqlType) {
        checkArgument(handles(sqlType));
        return new IntegerColumnBuilder(sqlType);
    }

    public static boolean handles(final int sqlType) {
        switch (sqlType) {
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
                return true;
            default:
                return false;
        }
    }

    private static class IntegerColumnBuilder implements ColumnBuilder<IntegerColumn> {
        private final List<Optional<Integer>> data = Lists.newArrayList();
        private final int columnType;

        public IntegerColumnBuilder(final int columnType) {
            this.columnType = columnType;
        }

        @Override
        public void addFromResultSet(final ResultSet resultSet, final int i) throws SQLException {
            final Integer aInteger = resultSet.getInt(i);
            if (resultSet.wasNull()) {
                data.add(Optional.<Integer>absent());
            } else {
                data.add(Optional.of(aInteger));
            }
        }

        @Override
        public IntegerColumn build() {
            return new IntegerColumn(columnType, data);
        }

        @Override
        public void clear() {
            data.clear();
        }
    }
}

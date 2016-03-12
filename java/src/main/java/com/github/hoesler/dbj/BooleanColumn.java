package com.github.hoesler.dbj;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ForwardingList;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Booleans;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public final class BooleanColumn extends ForwardingList<Optional<Boolean>> implements Column<Optional<Boolean>> {

    private final static Boolean NA = Boolean.FALSE;
    private final List<Optional<Boolean>> data;
    private final int sqlType;

    private BooleanColumn(final int sqlType, final Collection<Optional<Boolean>> values) {
        this.sqlType = sqlType;
        data = ImmutableList.copyOf(values);
    }

    @Override
    public int size() {
        return data.size();
    }

    @Override
    protected List<Optional<Boolean>> delegate() {
        return data;
    }

    @Override
    public Optional<Boolean> get(final int i) {
        return data.get(i);
    }

    public boolean[] toBooleans() {
        final boolean[] booleans = new boolean[size()];
        for (int i = 0; i < data.size(); i++) {
            booleans[i] = data.get(i).or(NA);
        }
        return booleans;
    }

    @Override
    public void updateStatement(final PreparedStatement statement, final int statementIndex, final int columnIndex) throws SQLException {
        final Optional<Boolean> aBoolean = data.get(columnIndex);
        if (!aBoolean.isPresent()) {
            statement.setNull(statementIndex, sqlType);
        } else {
            statement.setBoolean(statementIndex, aBoolean.get());
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

    public static BooleanColumn create(final int sqlType, final boolean[] booleans) {
        checkNotNull(booleans);
        checkArgument(handles(sqlType));

        return new BooleanColumn(sqlType, Lists.transform(Booleans.asList(booleans),
                new Function<Boolean, Optional<Boolean>>() {
                    @Override
                    public Optional<Boolean> apply(final Boolean aDouble) {
                        return Optional.of(aDouble);
                    }
                }));
    }

    /**
     * Create a new BooleanColumn from an array of boolean values.
     *
     * @param sqlType  the {@link java.sql.Types sql type} for this column
     * @param booleans Boolean values
     * @param na       NA indicators
     * @return a new Date column
     */
    public static BooleanColumn create(final int sqlType, final boolean[] booleans, final boolean[] na) {
        checkNotNull(booleans);
        checkNotNull(na);
        checkArgument(booleans.length == na.length);
        checkArgument(handles(sqlType));

        final ArrayList<Optional<Boolean>> optionals = new ArrayList<Optional<Boolean>>(booleans.length);
        for (int i = 0; i < booleans.length; i++) {
            optionals.add(na[i] ? Optional.<Boolean>absent() : Optional.of(booleans[i]));
        }
        return new BooleanColumn(sqlType, optionals);
    }

    public static ColumnBuilder<BooleanColumn> builder(final int sqlType) {
        checkArgument(handles(sqlType));
        return new BooleanColumnBuilder(sqlType);
    }

    public static boolean handles(final int sqlType) {
        switch (sqlType) {
            case Types.BIT:
            case Types.BOOLEAN:
                return true;
            default:
                return false;
        }
    }

    private static class BooleanColumnBuilder implements ColumnBuilder<BooleanColumn> {
        private final List<Optional<Boolean>> data = Lists.newArrayList();
        private final int sqlType;

        private BooleanColumnBuilder(final int sqlType) {this.sqlType = sqlType;}

        @Override
        public void addFromResultSet(final ResultSet resultSet, final int i) throws SQLException {
            final Boolean aBoolean = resultSet.getBoolean(i);
            if (resultSet.wasNull()) {
                data.add(Optional.<Boolean>absent());
            } else {
                data.add(Optional.of(aBoolean));
            }
        }

        @Override
        public BooleanColumn build() {
            return new BooleanColumn(sqlType, data);
        }

        @Override
        public void clear() {
            data.clear();
        }
    }

    @Override
    public String toString() {
        return "BooleanColumn{" +
                "data=" + data +
                ", sqlType=" + sqlType +
                '}';
    }
}

package com.github.hoesler.dbj;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ForwardingList;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Booleans;
import com.google.common.primitives.Longs;

import java.sql.*;
import java.sql.Date;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public final class LongColumn extends ForwardingList<Optional<Long>> implements Column<Optional<Long>> {

    private final static Long NA = Long.MIN_VALUE;
    private static final Calendar GMT = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
    private final List<Optional<Long>> data;
    private final int sqlType;
    private final StatementUpdater updater;

    private LongColumn(final int sqlType, final Collection<Optional<Long>> values) {
        this.sqlType = sqlType;
        data = ImmutableList.copyOf(values);
        updater = createUpdater(sqlType);
    }

    private StatementUpdater createUpdater(final int sqlType) {
        if (sqlType == Types.BIGINT) {
            return new BigIntUpdater();
        } else if (sqlType == Types.DATE) {
            return new DateUpdater();
        } else if (sqlType == Types.TIMESTAMP) {
            return new TimestampUpdater();
        } else if (sqlType == Types.TIME) {
            return new TimeUpdater();
        } else {
            throw new IllegalArgumentException("SQL type not supported by this column");
        }
    }

    @Override
    public int size() {
        return data.size();
    }

    @Override
    protected List<Optional<Long>> delegate() {
        return data;
    }

    @Override
    public Optional<Long> get(final int i) {
        return data.get(i);
    }

    public long[] toLongs() {
        final long[] longs = new long[size()];
        for (int i = 0; i < data.size(); i++) {
            longs[i] = data.get(i).or(NA);
        }
        return longs;
    }

    @Override
    public void updateStatement(final PreparedStatement statement, final int statementIndex, final int columnIndex) throws SQLException {
        updater.updateStatement(statement, statementIndex, columnIndex);
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


    public static LongColumn create(final int sqlType, long[] longs) {
        checkNotNull(longs);
        checkArgument(handles(sqlType));

        return new LongColumn(sqlType, Lists.transform(Longs.asList(longs),
                new Function<Long, Optional<Long>>() {
                    @Override
                    public Optional<Long> apply(final Long aLong) {
                        return Optional.of(aLong);
                    }
                }));
    }

    /**
     * Create a new LongColumn from an array of long values.
     *
     * @param sqlType the {@link java.sql.Types sql type} for this column
     * @param longs   integer values
     * @param na      NA indicators
     * @return a new Date column
     */
    public static LongColumn create(final int sqlType, long[] longs, boolean[] na) {
        checkNotNull(longs);
        checkNotNull(na);
        checkArgument(longs.length == na.length);
        checkArgument(handles(sqlType));

        final ArrayList<Optional<Long>> optionals = new ArrayList<Optional<Long>>(longs.length);
        for (int i = 0; i < longs.length; i++) {
            optionals.add(na[i] ? Optional.<Long>absent() : Optional.of(longs[i]));
        }
        return new LongColumn(sqlType, optionals);
    }

    public static ColumnBuilder<LongColumn> builder(final int sqlType) {
        checkArgument(handles(sqlType));
        return new LongColumnBuilder(sqlType);
    }

    public static boolean handles(final int sqlType) {
        switch (sqlType) {
            case Types.BIGINT:
            case Types.DATE:
            case Types.TIMESTAMP:
            case Types.TIME:
                return true;
            default:
                return false;
        }
    }

    private static class LongColumnBuilder implements ColumnBuilder<LongColumn> {
        private final List<Optional<Long>> data = Lists.newArrayList();
        private final int columnType;
        private final ResultSetTransfer<Optional<Long>> transfer;

        public LongColumnBuilder(final int columnType) {

            this.columnType = columnType;
            this.transfer = createTransfer(columnType);
        }

        private ResultSetTransfer<Optional<Long>> createTransfer(final int columnType) {
            if (columnType == Types.BIGINT) {
                return new BigIntResultSetTransfer();
            } else if (columnType == Types.DATE) {
                return new DateResultSetTransfer();
            } else if (columnType == Types.TIME) {
                return new TimeResultSetTransfer();
            } else if (columnType == Types.TIMESTAMP) {
                return new TimestampResultSetTransfer();
            } else {
                throw new IllegalArgumentException("Cannot create a result set transfer to double for sql type " + columnType);
            }
        }

        @Override
        public void addFromResultSet(final ResultSet resultSet, final int i) throws SQLException {
            transfer.addFromResultSet(data, resultSet, i);
        }

        @Override
        public LongColumn build() {
            return new LongColumn(columnType, data);
        }

        @Override
        public void clear() {
            data.clear();
        }

        private class BigIntResultSetTransfer implements ResultSetTransfer<Optional<Long>> {
            @Override
            public void addFromResultSet(final Collection<? super Optional<Long>> collection, final ResultSet resultSet, final int i) throws SQLException {
                final long aLong = resultSet.getLong(i);
                if (resultSet.wasNull()) {
                    collection.add(Optional.<Long>absent());
                } else {
                    collection.add(Optional.of(aLong));
                }
            }
        }

        private class DateResultSetTransfer implements ResultSetTransfer<Optional<Long>> {
            @Override
            public void addFromResultSet(final Collection<? super Optional<Long>> collection, final ResultSet resultSet, final int i) throws SQLException {
                final Date date = resultSet.getDate(i, GMT);
                if (resultSet.wasNull()) {
                    collection.add(Optional.<Long>absent());
                } else {
                    collection.add(Optional.of(date.getTime()));
                }
            }
        }

        private class TimeResultSetTransfer implements ResultSetTransfer<Optional<Long>> {
            @Override
            public void addFromResultSet(final Collection<? super Optional<Long>> collection, final ResultSet resultSet, final int i) throws SQLException {
                final Time time = resultSet.getTime(i);
                if (resultSet.wasNull()) {
                    collection.add(Optional.<Long>absent());
                } else {
                    collection.add(Optional.of(time.getTime()));
                }
            }
        }

        private class TimestampResultSetTransfer implements ResultSetTransfer<Optional<Long>> {
            @Override
            public void addFromResultSet(final Collection<? super Optional<Long>> collection, final ResultSet resultSet, final int i) throws SQLException {
                final Timestamp timestamp = resultSet.getTimestamp(i);
                if (resultSet.wasNull()) {
                    collection.add(Optional.<Long>absent());
                } else {
                    collection.add(Optional.of(timestamp.getTime()));
                }
            }
        }
    }

    private class BigIntUpdater implements StatementUpdater {

        @Override
        public void updateStatement(final PreparedStatement statement, final int statementIndex, final int columnIndex) throws SQLException {
            final Optional<Long> longOptional = data.get(columnIndex);
            if (!longOptional.isPresent()) {
                statement.setNull(statementIndex, sqlType);
            } else {
                statement.setLong(statementIndex, longOptional.get());
            }
        }
    }

    private class DateUpdater implements StatementUpdater {

        @Override
        public void updateStatement(final PreparedStatement statement, final int statementIndex, final int columnIndex) throws SQLException {
            final Optional<Long> longOptional = data.get(columnIndex);
            if (!longOptional.isPresent()) {
                statement.setNull(statementIndex, sqlType);
            } else {
                statement.setDate(statementIndex, new Date(longOptional.get()), GMT);
            }
        }
    }

    private class TimestampUpdater implements StatementUpdater {

        @Override
        public void updateStatement(final PreparedStatement statement, final int statementIndex, final int columnIndex) throws SQLException {
            final Optional<Long> longOptional = data.get(columnIndex);
            if (!longOptional.isPresent()) {
                statement.setNull(statementIndex, sqlType);
            } else {
                statement.setTimestamp(statementIndex, new Timestamp(longOptional.get()));
            }
        }
    }

    private class TimeUpdater implements StatementUpdater {

        @Override
        public void updateStatement(final PreparedStatement statement, final int statementIndex, final int columnIndex) throws SQLException {
            final Optional<Long> longOptional = data.get(columnIndex);
            if (!longOptional.isPresent()) {
                statement.setNull(statementIndex, sqlType);
            } else {
                statement.setTime(statementIndex, new Time(longOptional.get()));
            }
        }
    }

    @Override
    public String toString() {
        return "LongColumn{" +
                "sqlType=" + sqlType +
                ", data=" + data +
                '}';
    }
}

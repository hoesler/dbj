package com.github.hoesler.dbj;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ForwardingList;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Booleans;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public final class StringColumn extends ForwardingList<Optional<String>> implements Column<Optional<String>> {

    static final String NA = "NA";
    private final List<Optional<String>> data;
    private final int sqlType;
    private final StatementUpdater updater;

    private StringColumn(final int sqlType, final Collection<Optional<String>> values) {
        this.sqlType = sqlType;
        data = ImmutableList.copyOf(values);
        updater = createUpdater(sqlType);
    }

    private StatementUpdater createUpdater(final int sqlType) {
        if (sqlType == Types.VARCHAR || sqlType == Types.CHAR || sqlType == Types.LONGVARCHAR) {
            return new StringUpdater();
        } else if (sqlType == Types.NVARCHAR || sqlType == Types.NCHAR || sqlType == Types.LONGNVARCHAR) {
            return new NStringUpdater();
        } else {
            throw new IllegalArgumentException("SQL type not supported by this column");
        }
    }

    public String[] toStrings() {
        return FluentIterable.from(data)
                .transform(new Function<Optional<String>, String>() {
                    @Override
                    public String apply(final Optional<String> stringOptional) {
                        return stringOptional.or(NA);
                    }
                }).toArray(String.class);
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

    @Override
    protected List<Optional<String>> delegate() {
        return data;
    }

    @Override
    public Optional<String> get(final int i) {
        return data.get(i);
    }

    @Override
    public int size() {
        return data.size();
    }

    public static StringColumn create(final int sqlType, String[] longs) {
        checkNotNull(longs);
        checkArgument(handles(sqlType));

        return new StringColumn(sqlType, Lists.transform(Arrays.asList(longs),
                new Function<String, Optional<String>>() {
                    @Override
                    public Optional<String> apply(final String aString) {
                        return Optional.of(aString);
                    }
                }));
    }

    /**
     * Create a new StringColumn from an array of Strings.
     *
     * @param sqlType the {@link java.sql.Types sql type} for this column
     * @param strings String values
     * @param na      NA indicators
     * @return a new Date column
     */
    public static StringColumn create(final int sqlType, String[] strings, boolean[] na) {
        checkNotNull(strings);
        checkNotNull(na);
        checkArgument(strings.length == na.length);

        final ArrayList<Optional<String>> stringOptionals = new ArrayList<Optional<String>>(strings.length);
        for (int i = 0; i < strings.length; i++) {
            stringOptionals.add(na[i] ? Optional.<String>absent() : Optional.of(strings[i]));
        }
        return new StringColumn(sqlType, stringOptionals);
    }

    public static ColumnBuilder<StringColumn> builder(final int columnType) {
        return new StringColumnBuilder(columnType);
    }

    public static boolean handles(final int sqlType) {
        switch (sqlType) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
                return true;
            default:
                return false;
        }
    }

    private static class StringColumnBuilder implements ColumnBuilder<StringColumn> {
        private final List<Optional<String>> data = Lists.newArrayList();
        private final int columnType;
        private final ResultSetTransfer<Optional<String>> transfer;

        public StringColumnBuilder(final int columnType) {

            this.columnType = columnType;
            this.transfer = createTransfer(columnType);
        }

        private ResultSetTransfer<Optional<String>> createTransfer(final int columnType) {
            if (columnType == Types.CHAR || columnType == Types.VARCHAR || columnType == Types.LONGVARCHAR) {
                return new StringResultSetTransfer();
            } else if (columnType == Types.NCHAR || columnType == Types.NVARCHAR || columnType == Types.LONGNVARCHAR) {
                return new NStringResultSetTransfer();
            } else {
                throw new IllegalArgumentException("Cannot create a result set transfer to double for sql type " + columnType);
            }
        }

        @Override
        public void addFromResultSet(final ResultSet resultSet, final int i) throws SQLException {
            transfer.addFromResultSet(data, resultSet, i);
        }

        @Override
        public StringColumn build() {
            return new StringColumn(columnType, data);
        }

        @Override
        public void clear() {
            data.clear();
        }

        private class StringResultSetTransfer implements ResultSetTransfer<Optional<String>> {
            @Override
            public void addFromResultSet(final Collection<? super Optional<String>> collection, final ResultSet resultSet, final int i) throws SQLException {
                final String string = resultSet.getString(i);
                if (resultSet.wasNull()) {
                    data.add(Optional.<String>absent());
                } else {
                    data.add(Optional.of(string));
                }
            }
        }

        private class NStringResultSetTransfer implements ResultSetTransfer<Optional<String>> {

            @Override
            public void addFromResultSet(final Collection<? super Optional<String>> collection, final ResultSet resultSet, final int i) throws SQLException {
                final String nString = resultSet.getNString(i);
                if (resultSet.wasNull()) {
                    data.add(Optional.<String>absent());
                } else {
                    data.add(Optional.of(nString));
                }
            }
        }
    }

    private class StringUpdater implements StatementUpdater {

        @Override
        public void updateStatement(final PreparedStatement statement, final int statementIndex, final int columnIndex) throws SQLException {
            final Optional<String> optional = data.get(columnIndex);
            if (!optional.isPresent()) {
                statement.setObject(statementIndex, null);
            } else {
                statement.setString(statementIndex, optional.get());
            }
        }
    }

    private class NStringUpdater implements StatementUpdater {

        @Override
        public void updateStatement(final PreparedStatement statement, final int statementIndex, final int columnIndex) throws SQLException {
            final Optional<String> optional = data.get(columnIndex);
            if (!optional.isPresent()) {
                statement.setObject(statementIndex, null);
            } else {
                statement.setNString(statementIndex, optional.get());
            }
        }
    }
}

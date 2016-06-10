package com.github.hoesler.dbj;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ForwardingList;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

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

public final class BinaryColumn extends ForwardingList<Optional<byte[]>> implements Column<Optional<byte[]>> {

    private final static byte[] NA = new byte[0];
    private final List<Optional<byte[]>> data;
    private final int sqlType;

    private BinaryColumn(final int sqlType, final Collection<Optional<byte[]>> values) {
        this.sqlType = sqlType;
        data = ImmutableList.copyOf(values);
    }

    @Override
    public int size() {
        return data.size();
    }

    @Override
    protected List<Optional<byte[]>> delegate() {
        return data;
    }

    @Override
    public Optional<byte[]> get(final int i) {
        return data.get(i);
    }

    public byte[][] toByteArrays() {
        final byte[][] booleans = new byte[size()][];
        for (int i = 0; i < data.size(); i++) {
            booleans[i] = data.get(i).or(NA);
        }
        return booleans;
    }

    @Override
    public void updateStatement(final PreparedStatement statement, final int statementIndex, final int columnIndex) throws SQLException {
        final Optional<byte[]> aBoolean = data.get(columnIndex);
        if (!aBoolean.isPresent()) {
            statement.setObject(statementIndex, null);
        } else {
            statement.setBytes(statementIndex, aBoolean.get());
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

    public static BinaryColumn create(final int sqlType, final Object[] byteArrayArray) {
        checkNotNull(byteArrayArray);
        checkArgument(handles(sqlType));

        return new BinaryColumn(sqlType, Lists.transform(Arrays.asList(byteArrayArray),
                new Function<Object, Optional<byte[]>>() {
                    @Override
                    public Optional<byte[]> apply(final Object aDouble) {
                        return Optional.of((byte[]) aDouble);
                    }
                }));
    }

    /**
     * Create a new BooleanColumn from an array of boolean values.
     *
     * @param sqlType        the {@link Types sql type} for this column
     * @param byteArrayArray byte array values
     * @param na             NA indicators
     * @return a new Date column
     */
    public static BinaryColumn create(final int sqlType, final Object[] byteArrayArray, final boolean[] na) {
        checkNotNull(byteArrayArray);
        checkNotNull(na);
        checkArgument(byteArrayArray.length == na.length);
        checkArgument(handles(sqlType));

        final ArrayList<Optional<byte[]>> optionals = new ArrayList<Optional<byte[]>>(byteArrayArray.length);
        for (int i = 0; i < byteArrayArray.length; i++) {
            optionals.add(na[i] ? Optional.<byte[]>absent() : Optional.of((byte[]) byteArrayArray[i]));
        }
        return new BinaryColumn(sqlType, optionals);
    }

    public static ColumnBuilder<BinaryColumn> builder(final int sqlType) {
        checkArgument(handles(sqlType));
        return new BinaryColumnBuilder(sqlType);
    }

    public static boolean handles(final int sqlType) {
        switch (sqlType) {
            case Types.BINARY:
            case Types.BLOB:
                return true;
            default:
                return false;
        }
    }

    private static class BinaryColumnBuilder implements ColumnBuilder<BinaryColumn> {
        private final List<Optional<byte[]>> data = Lists.newArrayList();
        private final int sqlType;

        private BinaryColumnBuilder(final int sqlType) {this.sqlType = sqlType;}

        @Override
        public void addFromResultSet(final ResultSet resultSet, final int i) throws SQLException {
            final byte[] aBoolean = resultSet.getBytes(i);
            if (resultSet.wasNull()) {
                data.add(Optional.<byte[]>absent());
            } else {
                data.add(Optional.of(aBoolean));
            }
        }

        @Override
        public BinaryColumn build() {
            return new BinaryColumn(sqlType, data);
        }

        @Override
        public void clear() {
            data.clear();
        }
    }

    @Override
    public String toString() {
        return "BinaryColumn{" +
                "data=" + data +
                ", sqlType=" + sqlType +
                '}';
    }
}

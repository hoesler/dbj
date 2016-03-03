package com.github.hoesler.dbj;

import com.google.common.base.Optional;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.AbstractList;
import java.util.Arrays;

public class NullColumn extends AbstractList<Optional<Void>> implements Column<Optional<Void>> {

    private final int columnLength;

    private NullColumn(final int columnLength) {
        this.columnLength = columnLength;
    }

    public static NullColumn create(final int columnLength) {return new NullColumn(columnLength);}

    @Override
    public boolean[] getNA() {
        final boolean[] booleans = new boolean[columnLength];
        Arrays.fill(booleans, false);
        return booleans;
    }

    @Override
    public int sqlType() {
        return Types.NULL;
    }

    @Override
    public String getSimpleClassName() {
        return getClass().getSimpleName();
    }

    @Override
    public void updateStatement(final PreparedStatement statement, final int statementIndex, final int columnIndex) throws SQLException {
        statement.setNull(statementIndex, 0);
    }

    public static boolean handles(final int sqlType) {
        return sqlType == Types.NULL;
    }

    public static ColumnBuilder<NullColumn> builder(final int sqlType) {
        return new NullColumnBuilder();
    }

    @Override
    public Optional<Void> get(final int i) {
        return Optional.absent();
    }

    @Override
    public int size() {
        return columnLength;
    }

    private static class NullColumnBuilder implements ColumnBuilder<NullColumn> {
        private int count;

        @Override
        public NullColumn build() {
            return create(count);
        }

        @Override
        public void clear() {
            count = 0;
        }

        @Override
        public void addFromResultSet(final ResultSet resultSet, final int i) throws SQLException {
            count++;
        }
    }
}

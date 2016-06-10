package com.github.hoesler.dbj;

import com.google.common.base.Optional;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.AbstractList;

public class NullColumn extends AbstractList<Optional<Void>> implements Column<Optional<Void>> {

    private final int columnLength;

    private NullColumn(final int columnLength) {
        this.columnLength = columnLength;
    }

    public static NullColumn create(final int columnLength) {return new NullColumn(columnLength);}

    @Override
    public boolean[] getNA() {
        return new boolean[size()];
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
        statement.setNull(statementIndex, Types.NULL);
    }

    public static boolean handles(final int sqlType) {
        return sqlType == Types.NULL;
    }

    public static ColumnBuilder<NullColumn> builder() {
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

    @Override
    public String toString() {
        return "NullColumn{" +
                "columnLength=" + columnLength +
                '}';
    }
}

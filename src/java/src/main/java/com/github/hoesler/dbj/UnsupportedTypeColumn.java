package com.github.hoesler.dbj;

import com.google.common.base.Optional;
import com.google.common.collect.ForwardingList;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class UnsupportedTypeColumn extends ForwardingList<Optional<Object>> implements Column<Optional<Object>> {
    private final int sqlType;
    private final List<Optional<Object>> data;

    private UnsupportedTypeColumn(final int sqlType, final List<Optional<Object>> data) {
        this.sqlType = sqlType;
        this.data = ImmutableList.copyOf(data);
    }

    @Override
    public boolean[] getNA() {
        return new boolean[size()];
    }

    @Override
    public int sqlType() {
        return sqlType;
    }

    @Override
    public String getSimpleClassName() {
        return UnsupportedTypeColumn.class.getSimpleName();
    }

    @Override
    protected List<Optional<Object>> delegate() {
        return data;
    }

    @Override
    public void updateStatement(final PreparedStatement statement, final int statementIndex, final int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not supported");
    }

    public static ColumnBuilder<?> builder(final int sqlType) {
        return new UnsupportedTypeColumnBuilder(sqlType);
    }

    private static class UnsupportedTypeColumnBuilder implements ColumnBuilder<UnsupportedTypeColumn> {
        private final List<Optional<Object>> data = Lists.newArrayList();
        private final int sqlType;

        private UnsupportedTypeColumnBuilder(final int sqlType) {this.sqlType = sqlType;}


        @Override
        public void addFromResultSet(final ResultSet resultSet, final int i) throws SQLException {
            if (resultSet.wasNull()) {
                data.add(Optional.absent());
            } else {
                data.add(Optional.of(resultSet.getObject(i)));
            }
        }

        @Override
        public UnsupportedTypeColumn build() {
            return new UnsupportedTypeColumn(sqlType, data);
        }

        @Override
        public void clear() {
            data.clear();
        }
    }
}

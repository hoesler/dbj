package com.github.hoesler.dbj;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;

/**
 * JDBCResultPull is a helper class for fetching data from {@link java.sql.ResultSet result sets} and storing these into
 * column arrays. This moves the expensive row iteration from R to Java and allows a performance optimized data transfer
 * from Java to R.
 */
public final class JDBCResultPull {
    /**
     * active result set
     */
    private final ResultSet resultSet;
    private final ArrayList<ColumnBuilder<?>> columnBuilders;

    /**
     * create a JDBCResultPull from teh current set with the specified column types. The column type definition must
     * match the result set, no checks are performed.
     *
     * @param resultSet active result set
     */
    public JDBCResultPull(final ResultSet resultSet) throws SQLException {
        if (resultSet == null) {
            throw new NullPointerException("Result set must not be null");
        }
        this.resultSet = resultSet;
        this.columnBuilders = initColumns();
    }

    /**
     * Allocate arrays for the given capacity. Normally this method is not called directly since @link{fetch()}
     * automatically allocates necessary space, but it can be used to reduce the array sizes when idle (e.g., by setting
     * the capacity to 0).
     *
     * @throws SQLException if an SQL exception occurs
     */
    private ArrayList<ColumnBuilder<?>> initColumns() throws SQLException {
        final ArrayList<ColumnBuilder<?>> columns = new ArrayList<ColumnBuilder<?>>();
        final ResultSetMetaData metaData = resultSet.getMetaData();
        for (int i = 0; i < metaData.getColumnCount(); i++) {
            final int columnType = metaData.getColumnType(i + 1);
            final ColumnBuilder<?> builder = createColumnBuilder(columnType);
            columns.add(builder);
        }
        return columns;
    }

    private static ColumnBuilder<?> createColumnBuilder(final int sqlType) {
        if (BooleanColumn.handles(sqlType)) {
            return BooleanColumn.builder(sqlType);
        } else if (IntegerColumn.handles(sqlType)) {
            return IntegerColumn.builder(sqlType);
        } else if (LongColumn.handles(sqlType)) {
            return LongColumn.builder(sqlType);
        } else if (DoubleColumn.handles(sqlType)) {
            return DoubleColumn.builder(sqlType);
        } else if (StringColumn.handles(sqlType)) {
            return StringColumn.builder(sqlType);
        } else {
            return UnsupportedTypeColumn.builder(sqlType);
        }
    }

    /**
     * Fetch records from the result set into column arrays. It replaces any existing data in the buffers.
     *
     * @param atMost the maximum number of rows to be retrieved
     * @return number of rows retrieved
     * @throws SQLException if an SQL exception occurs
     */
    public Table fetch(final int atMost) throws SQLException {
        return fetch(atMost, 0);
    }

    /**
     * Fetch records from the result set into column arrays. It replaces any existing data in the buffers.
     *
     * @param atMost    the maximum number of rows to be retrieved
     * @param fetchSize fetch size hint to be sent to the driver. Note that some databases don't support fetch sizes
     *                  larger than 32767. If less than 1 the fetch size is not changed.
     * @return number of rows retrieved
     * @throws SQLException if an SQL exception occurs
     */
    public Table fetch(final int atMost, int fetchSize) throws SQLException {
        if (atMost < 0) {
            throw new IllegalArgumentException("atMost must be >= 0, was " + atMost);
        }

        try {
            resultSet.setFetchSize(fetchSize);
        } catch (SQLFeatureNotSupportedException ignored) {
        }

        for (ColumnBuilder<?> columnBuilder : columnBuilders) {
            columnBuilder.clear();
        }

        int rowCount = 0;
        while (resultSet.next() && rowCount < atMost) {
            for (int i = 0; i < columnBuilders.size(); i++) {
                final ColumnBuilder<?> columnBuilder = columnBuilders.get(i);
                columnBuilder.addFromResultSet(resultSet, i + 1);
            }
            rowCount++;
        }
        return ArrayListTable.create(Lists.transform(columnBuilders, new Function<ColumnBuilder<?>, Column<?>>() {
            @Override
            public Column<?> apply(final ColumnBuilder<?> columnBuilder) {
                return columnBuilder.build();
            }
        }));
    }

    public int[] getSqlTypes() throws SQLException {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final int columnCount = metaData.getColumnCount();
        final int[] sqlTypes = new int[columnCount];
        for (int i = 0; i < columnCount; i++) {
            sqlTypes[i] = metaData.getColumnType(i);
        }
        return sqlTypes;
    }
}

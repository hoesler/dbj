package info.urbanek.Rpackage.RJDBC;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;

/**
 * JDBCResultPull is a helper class for fetching data from {@link java.sql.ResultSet result sets} and storing these into
 * column arrays. This moves the expensive row iteration from R to Java and allows a performance optimized data transfer
 * from Java to R.
 */
public final class JDBCResultPull {

    /**
     * NA double value
     */
    private static final double NA_DOUBLE = Double.longBitsToDouble(0x7ff00000000007a2L);

    /**
     * active result set
     */
    private final ResultSet resultSet;

    /**
     * create a JDBCResultPull from teh current set with the specified column types. The column type definition must
     * match the result set, no checks are performed.
     *
     * @param resultSet active result set
     */
    public JDBCResultPull(final ResultSet resultSet) {
        if (resultSet == null) {
            throw new NullPointerException("Result set must not be null");
        }
        this.resultSet = resultSet;
    }

    /**
     * Allocate arrays for the given capacity. Normally this method is not called directly since @link{fetch()}
     * automatically allocates necessary space, but it can be used to reduce the array sizes when idle (e.g., by setting
     * the capacity to 0).
     *
     * @throws SQLException if an SQL exception occurs
     */
    private ArrayList<Column<?>> initColumns() throws SQLException {
        final ArrayList<Column<?>> columns = new ArrayList<Column<?>>();
        final ResultSetMetaData metaData = resultSet.getMetaData();
        for (int i = 0; i < metaData.getColumnCount(); i++) {
            final Column<?> column;
            final int columnType = metaData.getColumnType(i + 1);
            if (columnType == Types.BIGINT || columnType == Types.TINYINT
                    || (columnType >= Types.NUMERIC && columnType <= Types.DOUBLE)) {
                column = new DoubleColumn();
            } else {
                column = new StringColumn();
            }
            columns.add(column);
        }
        return columns;
    }

    /**
     * Fetch records from the result set into column arrays. It replaces any existing data in the buffers.
     *
     * @param atMost the maximum number of rows to be retrieved
     * @return number of rows retrieved
     * @throws SQLException if an SQL exception occurs
     */
    @SuppressWarnings("unchecked") // checked by column type
    public Table fetch(final int atMost) throws SQLException {
        if (atMost < 0) {
            throw new IllegalArgumentException("atMost must be >= 0, was " + atMost);
        }
        final ArrayList<Column<?>> columns = initColumns();
        resultSet.setFetchSize(atMost);
        int rowCount = 0;
        while (resultSet.next() && rowCount < atMost) {
            for (int i = 0; i < columns.size(); i++) {
                final Column<?> column = columns.get(i);
                column.addFromResultSet(resultSet, i + 1);
            }
            rowCount++;
        }
        return new ArrayListTable(columns);
    }
}

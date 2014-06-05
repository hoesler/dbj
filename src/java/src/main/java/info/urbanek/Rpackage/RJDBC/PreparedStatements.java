package info.urbanek.Rpackage.RJDBC;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class PreparedStatements {

    public static void batchInsert(final PreparedStatement statement, final Table table) throws SQLException {
        for (int i = 0; i < table.rowCount(); i++) {
            insert(statement, table, i);
            statement.addBatch();
        }
    }

    public static void insert(final PreparedStatement statement, final Table table, int rowIndex) throws SQLException {
        for (int j = 0; j < table.columnCount(); j++) {
            final Column<?> column = table.getColumn(j);
            column.update(statement, j + 1, rowIndex);
        }
    }
}

package info.urbanek.Rpackage.RJDBC;

import java.util.List;

public final class ArrayListTable implements Table {

    private final List<Column<?>> columns;

    /**
     * number of loaded rows
     */
    private final int rowCount;

    public ArrayListTable(final List<Column<?>> columns) {
        this.columns = columns;
        int rowCount = 0;
        for (Column<?> column : columns) {
            if (rowCount == 0) {
                rowCount = column.size();
            } else {
                if (column.size() != rowCount) {
                    throw new IllegalArgumentException("Columns have unequal size");
                }
            }
        }
        this.rowCount = rowCount;
    }

    public int rowCount() {
        return rowCount;
    }

    public int columnCount() {
        return columns.size();
    }

    public Column<?> getColumn(final int index) {

        if (index < 0 || index > columns.size()) {
            throw new IndexOutOfBoundsException();
        }
        return columns.get(index);
    }

}

package info.urbanek.Rpackage.RJDBC;

import java.util.*;

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

    @Override
    public Iterable<Column<?>> columns() {
        return columns;
    }

    @Override
    public Iterable<Row> rows() {
        return new Iterable<Row>() {
            @Override
            public Iterator<Row> iterator() {
                return new RowIterator();
            }
        };
    }

    public static ArrayListTable create(final Column<?>... columns) {
        return new ArrayListTable(Arrays.asList(columns));
    }

    private static class RowView extends AbstractList<Object> implements Row {
        private final Table table;
        private final int row;

        public RowView(final Table table, final int row) {
            this.table = table;
            this.row = row;
        }

        @Override
        public Object get(final int i) {
            return table.getColumn(i).get(row);
        }

        @Override
        public int size() {
            return table.rowCount();
        }
    }

    private class RowIterator implements Iterator<Row> {
        int iterated = 0;

        @Override
        public boolean hasNext() {
            return iterated < rowCount();
        }

        @Override
        public Row next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            final RowView rowView = new RowView(ArrayListTable.this, iterated);

            iterated++;

            return rowView;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}

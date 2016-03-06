package com.github.hoesler.dbj;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;

import java.util.*;

public final class ArrayListTable implements Table {

    private final List<Column<?>> columns;

    /**
     * number of loaded rows
     */
    private final int rowCount;

    public ArrayListTable(final List<Column<?>> columns) {
        this.columns = columns;
        this.rowCount = rows(columns);
    }

    private static int rows(final Collection<? extends Column<?>> columns) {
        if (columns.size() == 0) {
            return 0;
        } else {
            final Iterable<Integer> columnSizes = Iterables.transform(
                    columns, new Function<Column<?>, Integer>() {
                        @Override
                        public Integer apply(final Column<?> objects) {
                            return objects.size();
                        }
                    }
            );
            final ImmutableSet<Integer> sizesSet = ImmutableSet.copyOf(columnSizes);

            if (sizesSet.size() > 1) {
                throw new IllegalArgumentException("Columns have unequal size: " + columns);
            }

            return Iterables.getOnlyElement(sizesSet);
        }
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

    @Override
    public int[] sqlTypes() {
        return Ints.toArray(Lists.transform(columns, new Function<Column<?>, Integer>() {
            @Override
            public Integer apply(final Column<?> objects) {
                return objects.sqlType();
            }
        }));
    }

    public static ArrayListTable create(final Column<?>... columns) {
        return new ArrayListTable(Arrays.asList(columns));
    }

    public static ArrayListTable create(final List<Column<?>> columns) {
        return new ArrayListTable(columns);
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

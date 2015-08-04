package com.github.hoesler.dbj;

public interface Table {
    int rowCount();

    int columnCount();

    Column<?> getColumn(int index);

    Iterable<Column<?>> columns();

    Iterable<Row> rows();

    /**
     * Return the {@link java.sql.Types sql type} for each column.
     */
    int[] sqlTypes();
}

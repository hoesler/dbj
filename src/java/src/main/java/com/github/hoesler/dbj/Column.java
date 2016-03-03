package com.github.hoesler.dbj;

import java.util.List;

public interface Column<T> extends List<T>, StatementUpdater {

    /**
     * Create a boolean array indicating if rows are present or absent.
     * @return a boolean array indicating if rows are present or absent
     */
    boolean[] getNA();

    /**
     * Get the SQL type of this column
     * @return an {@link java.sql.Types SQL type}
     */
    int sqlType();

    /**
     * Get the simple class name of this column.
     * @return a class name
     */
    String getSimpleClassName();
}

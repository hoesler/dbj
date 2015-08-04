package com.github.hoesler.dbj;

import java.util.List;

public interface Column<T> extends List<T>, StatementUpdater {

    boolean[] getNA();

    int sqlType();

    String getSimpleClassName();
}

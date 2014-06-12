package info.urbanek.Rpackage.RJDBC;

import java.util.List;

public interface Column<T> extends List<T>, StatementUpdater {

    boolean[] getNA();

    int sqlType();

    String getSimpleClassName();
}

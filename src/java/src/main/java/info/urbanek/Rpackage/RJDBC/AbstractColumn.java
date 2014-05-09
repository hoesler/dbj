package info.urbanek.Rpackage.RJDBC;

import java.util.ArrayList;

public abstract class AbstractColumn<T> implements Column<T> {
    protected final ArrayList<T> data = new ArrayList<T>();

    public final void add(final T value) {
        data.add(value);
    }

    public final void set(final int index, final T value) {
        data.add(value);
    }

    public final void clear() {
        data.clear();
    }

    public final int size() {
        return data.size();
    }
}

package info.urbanek.Rpackage.RJDBC;

public interface Table {
    int rowCount();

    int columnCount();

    Column<?> getColumn(int index);
}
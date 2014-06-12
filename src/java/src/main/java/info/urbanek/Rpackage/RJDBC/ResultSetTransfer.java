package info.urbanek.Rpackage.RJDBC;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

public interface ResultSetTransfer<T> {
    void addFromResultSet(Collection<? super T> collection, ResultSet resultSet, int i) throws SQLException;
}

package com.github.hoesler.dbj;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

public interface ResultSetTransfer<T> {
    void addFromResultSet(Collection<? super T> collection, ResultSet resultSet, int i) throws SQLException;
}

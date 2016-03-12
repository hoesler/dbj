package com.github.hoesler.dbj;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface ColumnBuilder<T extends Column<?>> {

    T build();

    void clear();

    void addFromResultSet(ResultSet resultSet, int i) throws SQLException;
}

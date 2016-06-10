package com.github.hoesler.dbj;

import org.junit.Rule;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Types;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class DoubleColumnTest {
    @Rule
    public H2Database database = new H2Database();

    @Test
    public void testUpdateStatement() throws Exception {
        // given
        final DoubleColumn doubleColumn = DoubleColumn.create(Types.DOUBLE,
                new double[]{42.0},
                new boolean[]{false});
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);

        // when
        doubleColumn.updateStatement(preparedStatement, 0, 0);

        // then
        verify(preparedStatement).setDouble(0, 42.0);
    }

    @Test
    public void testUpdateStatementNull() throws Exception {
        // given
        final DoubleColumn doubleColumn = DoubleColumn.create(Types.DOUBLE,
                new double[]{42.0},
                new boolean[]{true});
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);

        // when
        doubleColumn.updateStatement(preparedStatement, 0, 0);

        // then
        verify(preparedStatement).setObject(0, null);
    }
}
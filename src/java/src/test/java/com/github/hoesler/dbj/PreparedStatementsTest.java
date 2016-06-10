package com.github.hoesler.dbj;

import org.junit.Rule;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Types;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class PreparedStatementsTest {
    @Rule
    public H2Database database = new H2Database();

    @Test
    public void testBatchInsert() throws Exception {
        // given
        final Connection connection = database.getConnection();
        connection.createStatement().execute(
                "CREATE TABLE \"test_table\" (\"a\" DOUBLE, \"b\" VARCHAR(255), \"c\" INTEGER, \"d\" DATE, \"e\" TIMESTAMP)");

        final ArrayListTable table = ArrayListTable.create(
                DoubleColumn.create(Types.DOUBLE,
                        new double[]{-42.0, 0, 42.0, Double.NaN, 1},
                        new boolean[]{false, false, false, false, true}),
                StringColumn.create(Types.VARCHAR,
                        new String[]{"a", "", "cb67%&$'", "NA", "fds"},
                        new boolean[]{false, false, false, false, true}),
                IntegerColumn.create(Types.INTEGER,
                        new int[]{-42, 0, 42, Integer.MAX_VALUE, 5},
                        new boolean[]{false, false, false, false, true}),
                LongColumn.create(Types.DATE,
                        new long[]{3447, 4476, -45334, 0, 5},
                        new boolean[]{false, false, false, false, true}),
                LongColumn.create(Types.TIMESTAMP,
                        new long[]{-42, 0, 42, Long.MAX_VALUE, 5},
                        new boolean[]{false, false, false, false, true})
        );

        final PreparedStatement preparedStatement =
                connection.prepareStatement(
                        "INSERT INTO \"test_table\" (\"a\", \"b\", \"c\", \"d\", \"e\") VALUES (?, ?, ?, ?, ?)");

        // when
        PreparedStatements.batchInsert(preparedStatement, table);

        // then
        final int[] ints = preparedStatement.executeBatch();
        assertThat(ints.length, is(5));
    }

    @Test
    public void testInsertTime() throws Exception {
        // given
        final Connection connection = database.getConnection();
        connection.createStatement().execute(
                "CREATE TABLE \"test_table\" (\"a\" TIME)");

        final ArrayListTable table = ArrayListTable.create(
                LongColumn.create(Types.TIME, new long[]{1000000})
        );

        final PreparedStatement preparedStatement =
                connection.prepareStatement(
                        "INSERT INTO \"test_table\" (\"a\") VALUES (?)");

        // when
        PreparedStatements.insert(preparedStatement, table, 0);

        // then
        final int affected = preparedStatement.executeUpdate();
        assertThat(affected, is(1));
    }
}
package com.github.hoesler.dbj;

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;

import java.sql.*;
import java.util.Arrays;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

public class JDBCResultPullTest {

    @Rule
    public H2Database database = new H2Database();

    @Test
    public void testFetchTime() throws Exception {
        // given
        final Connection connection = database.getConnection();
        connection.createStatement().execute(
                "CREATE TABLE \"test_table\" (" +
                        "\"a\" TIME)");
        final Time value = Time.valueOf("19:04:00");
        connection.createStatement().execute(
                "INSERT INTO \"test_table\" (\"a\")" +
                        " VALUES ('" + value + "')");
        final ResultSet resultSet = connection.createStatement().executeQuery("SELECT * FROM \"test_table\"");
        final JDBCResultPull pull = new JDBCResultPull(resultSet);

        // when
        final Table table = pull.fetch(1);

        // then
        assertThat(table, is(notNullValue()));
        assertThat(table.columnCount(), is(1));
        assertThat(table.rowCount(), is(1));
        assertThat(table.getColumn(0), is(instanceOf(LongColumn.class)));
        assertThat(((LongColumn) table.getColumn(0)).get(0).isPresent(), is(true));
        assertThat(((LongColumn) table.getColumn(0)).get(0).get(), is(equalTo(value.getTime())));
        assertThat(Longs.asList(((LongColumn) table.getColumn(0)).toLongs()), Matchers.contains(value.getTime()));
    }

    @Test
    public void testFetchTimestamp() throws Exception {
        // given
        final Connection connection = database.getConnection();
        connection.createStatement().execute(
                "CREATE TABLE \"test_table\" (" +
                        "\"a\" TIMESTAMP)");
        final Timestamp value = Timestamp.valueOf("2014-06-03 19:04:00");
        connection.createStatement().execute(
                "INSERT INTO \"test_table\" (\"a\")" +
                        " VALUES ('" + value + "')");
        final ResultSet resultSet = connection.createStatement().executeQuery("SELECT * FROM \"test_table\"");
        final JDBCResultPull pull = new JDBCResultPull(resultSet);

        // when
        final Table table = pull.fetch(1);

        // then
        assertThat(table, is(notNullValue()));
        assertThat(table.columnCount(), is(1));
        assertThat(table.rowCount(), is(1));
        assertThat(table.getColumn(0), is(instanceOf(LongColumn.class)));
        assertThat(((LongColumn) table.getColumn(0)).get(0).isPresent(), is(true));
        assertThat(((LongColumn) table.getColumn(0)).get(0).get(), is(equalTo(value.getTime())));
        assertThat(Longs.asList(((LongColumn) table.getColumn(0)).toLongs()), Matchers.contains(value.getTime()));
    }

    @Test
    public void testFetchDate() throws Exception {
        // given
        final Connection connection = database.getConnection();
        connection.createStatement().execute(
                "CREATE TABLE \"test_table\" (" +
                        "\"a\" DATE)");
        final String dateString = "2014-06-03";
        final Date value = Date.valueOf(dateString);
        connection.createStatement().execute(
                "INSERT INTO \"test_table\" (\"a\")" +
                        " VALUES ('" + value + "')");
        final ResultSet resultSet = connection.createStatement().executeQuery("SELECT * FROM \"test_table\"");
        final JDBCResultPull pull = new JDBCResultPull(resultSet);

        // when
        final Table table = pull.fetch(1);

        // then
        assertThat(table, is(notNullValue()));
        assertThat(table.columnCount(), is(1));
        assertThat(table.rowCount(), is(1));
        assertThat(table.getColumn(0), is(instanceOf(LongColumn.class)));
        assertThat(((LongColumn) table.getColumn(0)).get(0).isPresent(), is(true));
        assertThat(((LongColumn) table.getColumn(0)).get(0).get(), is(equalTo(value.getTime())));
        assertThat(Longs.asList(((LongColumn) table.getColumn(0)).toLongs()), Matchers.contains(value.getTime()));
    }

    @Test
    public void testFetchInteger() throws Exception {
        // given
        final Connection connection = database.getConnection();
        connection.createStatement().execute(
                "CREATE TABLE \"test_table\" (" +
                        "\"a\" INTEGER)");
        final int value = 42;
        connection.createStatement().execute(
                "INSERT INTO \"test_table\" (\"a\")" +
                        " VALUES (" + value + ")");
        final ResultSet resultSet = connection.createStatement().executeQuery("SELECT * FROM \"test_table\"");
        final JDBCResultPull pull = new JDBCResultPull(resultSet);

        // when
        final Table table = pull.fetch(1);

        // then
        assertThat(table, is(notNullValue()));
        assertThat(table.columnCount(), is(1));
        assertThat(table.rowCount(), is(1));
        assertThat(table.getColumn(0), is(instanceOf(IntegerColumn.class)));
        assertThat(((IntegerColumn) table.getColumn(0)).get(0).isPresent(), is(true));
        assertThat(((IntegerColumn) table.getColumn(0)).get(0).get(), is(equalTo(value)));
        assertThat(Ints.asList(((IntegerColumn) table.getColumn(0)).toInts()), Matchers.contains(value));
    }

    @Test
    public void testFetchIntegerChunks() throws Exception {
        // given
        final Connection connection = database.getConnection();
        connection.createStatement().execute(
                "CREATE TABLE \"test_table\" (" +
                        "\"a\" INTEGER)");
        for (int i = 0; i < 10; i++) {
            connection.createStatement().execute(
                    "INSERT INTO \"test_table\" (\"a\")" +
                            " VALUES (" + i + ")");
        }

        final ResultSet resultSet = connection.createStatement().executeQuery("SELECT * FROM \"test_table\"");
        final JDBCResultPull pull = new JDBCResultPull(resultSet);

        // when
        final Table table1 = pull.fetch(5);
        final Table table2 = pull.fetch(3);
        final Table table3 = pull.fetch(5);

        // then
        assertThat(table1.rowCount(), is(5));
        assertThat(table2.rowCount(), is(3));
        assertThat(table3.rowCount(), is(2));
    }

    @Test
    public void testFetchString() throws Exception {
        // given
        final Connection connection = database.getConnection();
        connection.createStatement().execute(
                "CREATE TABLE \"test_table\" (" +
                        "\"a\" VARCHAR(255))");
        final String value = "Hello World!";
        connection.createStatement().execute(
                "INSERT INTO \"test_table\" (\"a\")" +
                        " VALUES ('" + value + "')");
        final ResultSet resultSet = connection.createStatement().executeQuery("SELECT * FROM \"test_table\"");
        final JDBCResultPull pull = new JDBCResultPull(resultSet);

        // when
        final Table table = pull.fetch(1);

        // then
        assertThat(table, is(notNullValue()));
        assertThat(table.columnCount(), is(1));
        assertThat(table.rowCount(), is(1));
        assertThat(table.getColumn(0), is(instanceOf(StringColumn.class)));
        assertThat(((StringColumn) table.getColumn(0)).get(0).isPresent(), is(true));
        assertThat(((StringColumn) table.getColumn(0)).get(0).get(), is(equalTo(value)));
        assertThat(Arrays.asList((((StringColumn) table.getColumn(0)).toStrings())), Matchers.contains(value));
    }

    @Test
    public void testFetchDouble() throws Exception {
        // given
        final Connection connection = database.getConnection();
        connection.createStatement().execute(
                "CREATE TABLE \"test_table\" (" +
                        "\"a\" DOUBLE)");
        final double value = 42.0;
        connection.createStatement().execute(
                "INSERT INTO \"test_table\" (\"a\")" +
                        " VALUES (" + value + ")");
        final ResultSet resultSet = connection.createStatement().executeQuery("SELECT * FROM \"test_table\"");
        final JDBCResultPull pull = new JDBCResultPull(resultSet);

        // when
        final Table table = pull.fetch(1);

        // then
        assertThat(table, is(notNullValue()));
        assertThat(table.columnCount(), is(1));
        assertThat(table.rowCount(), is(1));
        assertThat(table.getColumn(0), is(instanceOf(DoubleColumn.class)));
        assertThat(((DoubleColumn) table.getColumn(0)).get(0).isPresent(), is(true));
        assertThat(((DoubleColumn) table.getColumn(0)).get(0).get(), is(equalTo(value)));
        assertThat(Doubles.asList(((DoubleColumn) table.getColumn(0)).toDoubles()), Matchers.contains(value));
    }

    @Test
    public void testFetchNULL() throws Exception {
        // given
        final Connection connection = database.getConnection();
        final ResultSet resultSet = connection.createStatement().executeQuery("SELECT NULL");
        final JDBCResultPull pull = new JDBCResultPull(resultSet);

        // when
        final Table table = pull.fetch(1);

        // then
        assertThat(table, is(notNullValue()));
        assertThat(table.columnCount(), is(1));
        assertThat(table.rowCount(), is(1));
        assertThat(table.getColumn(0), is(instanceOf(NullColumn.class)));
    }
}

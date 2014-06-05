package info.urbanek.Rpackage.RJDBC;

import org.hamcrest.Matchers;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

public class JDBCResultPullTest {
    @Test
    public void testFetch() throws Exception {
        // given
        Class.forName("org.h2.Driver");
        final String url = "jdbc:h2:mem:";
        final Connection connection = DriverManager.getConnection(url, "sa", "");
        connection.createStatement().execute(
                "CREATE TABLE \"test_table\" (\"a\" DOUBLE, \"b\" VARCHAR(255), \"c\" INTEGER, \"d\" DATE, \"e\" TIMESTAMP)");
        connection.createStatement().execute(
                "INSERT INTO \"test_table\" (\"a\", \"b\", \"c\", \"d\", \"e\") VALUES (42.0, 'test', 33, '2014-06-03', '2014-06-03 19:04:00')");
        final ResultSet resultSet = connection.createStatement().executeQuery("SELECT * FROM \"test_table\"");
        final JDBCResultPull pull = new JDBCResultPull(resultSet);

        // when
        final Table table = pull.fetch(1);

        // then
        assertThat(table, is(notNullValue()));
        assertThat(table.columnCount(), is(5));
        assertThat(table.rowCount(), is(1));
        assertThat(table.getColumn(0), is(instanceOf(DoubleColumn.class)));
        assertThat(table.getColumn(1), is(instanceOf(StringColumn.class)));
        assertThat(table.getColumn(2), is(instanceOf(IntegerColumn.class)));
        assertThat(table.getColumn(3), is(instanceOf(DateColumn.class)));
        assertThat(table.getColumn(4), is(instanceOf(TimestampColumn.class)));
    }
}

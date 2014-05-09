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
        final ResultSet resultSet = connection.createStatement().executeQuery("SELECT 1 + 1, 'hello world'");
        final JDBCResultPull pull = new JDBCResultPull(resultSet);

        // when
        final Table table = pull.fetch(1);

        // then
        assertThat(table, is(notNullValue()));
        assertThat(table.columnCount(), is(2));
        assertThat(table.rowCount(), is(1));
        assertThat(table.getColumn(0), is(instanceOf(DoubleColumn.class)));
        assertThat(table.getColumn(0).toObjectArray(), is(Matchers.<Object>arrayContaining(2.0)));
        assertThat(table.getColumn(1), is(instanceOf(StringColumn.class)));
        assertThat(table.getColumn(1).toObjectArray(), is(Matchers.<Object>arrayContaining("hello world")));
    }
}

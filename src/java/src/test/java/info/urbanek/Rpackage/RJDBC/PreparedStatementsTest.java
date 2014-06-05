package info.urbanek.Rpackage.RJDBC;

import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class PreparedStatementsTest {

    @Test
    public void testBatchInsert() throws Exception {
        // given
        Class.forName("org.h2.Driver");
        final String url = "jdbc:h2:mem:";
        final Connection connection = DriverManager.getConnection(url, "sa", "");
        connection.createStatement().execute(
                "CREATE TABLE \"test_table\" (\"a\" DOUBLE, \"b\" VARCHAR(255), \"c\" INTEGER, \"d\" DATE, \"e\" TIMESTAMP)");

        final ArrayListTable table = ArrayListTable.create(
                DoubleColumn.create(new double[]{-42.0, 0, 42.0, Double.NaN}, new boolean[] {false, false, false, false}),
                StringColumn.create(new String[]{"a", "", "cb67%&$'", "NA"}, new boolean[] {false, false, false, false}),
                IntegerColumn.create(new int[]{-42, 0, 42, Integer.MAX_VALUE}, new boolean[] {false, false, false, false}),
                DateColumn.forDays(new int[]{3447, 4476, -45334, 0}, new boolean[] {false, false, false, false}),
                TimestampColumn.forSeconds(new int[]{-42, 0, 42, Integer.MAX_VALUE}, new boolean[] {false, false, false, false})
        );

        final PreparedStatement preparedStatement =
                connection.prepareStatement(
                        "INSERT INTO \"test_table\" (\"a\", \"b\", \"c\", \"d\", \"e\") VALUES (?, ?, ?, ?, ?)");

        // when
        PreparedStatements.batchInsert(preparedStatement, table);

        // then
        final int[] ints = preparedStatement.executeBatch();
        assertThat(ints.length, is(4));
    }
}
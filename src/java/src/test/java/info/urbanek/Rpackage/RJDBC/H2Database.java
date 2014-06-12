package info.urbanek.Rpackage.RJDBC;

import com.google.common.base.Throwables;
import org.junit.rules.ExternalResource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

class H2Database extends ExternalResource {

    private Connection connection;

    H2Database() {
        try {
            Class.forName("org.h2.Driver");
        } catch (ClassNotFoundException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    protected void before() throws Throwable {
        final String url = "jdbc:h2:mem:";
        connection = DriverManager.getConnection(url, "sa", "");
    }

    @Override
    protected void after() {
        try {
            connection.close();
        } catch (SQLException e) {
            // IGNORE
        }
    }

    public Connection getConnection() {
        return connection;
    }
}

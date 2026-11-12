import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Small connection helper for the preserved MySQL tables. The original project
 * opened a fresh connection per DAO call; that behavior is retained to avoid
 * adding a connection-pool dependency to this refactor.
 */
public final class Database {

    private static final AppConfig CONFIG = AppConfig.getInstance();

    static {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private Database() {
    }

    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(
                CONFIG.getDbUrl(),
                CONFIG.getDbUsername(),
                CONFIG.getDbPassword());
    }
}

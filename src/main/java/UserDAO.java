import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class UserDAO {

    public Connection getConnection() {
        try {
            return Database.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException("Unable to connect to the user database", e);
        }
    }

    //    Create table users(id INT PRIMARY KEY AUTO_INCREMENT,username varchar(20), password varchar(20));
    // store the new user in db
    public boolean userExist(String username) {
        //check existing user or not
        String sql = "SELECT * FROM users where username = ?";

        try (Connection connection = getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, username);

            ResultSet rs = stmt.executeQuery();

            if (rs.next()) {
                return rs.getString(2) != null;
            } else {
                return false;
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    public boolean registerUser(String username, String password) {
        //check existing user or not
        String sql = "INSERT INTO users(username , password) values(?,?)";

        try (Connection connection = getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, username);
            stmt.setString(2, password);

            int rs = stmt.executeUpdate();
            return rs != 0;

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    // get the user details is valid or not during login
    public int validateUser(String username, String password) {
        //check existing user or not
        String sql = "SELECT id FROM users where username =? and password = ?";

        try (Connection connection = getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, username);
            stmt.setString(2, password);

            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getInt(1);
            } else {
                return 0;
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public int getUserID(String username) {
        String sql = "SELECT id FROM users where username =?";

        try (Connection connection = getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, username);

            ResultSet rs = stmt.executeQuery();

            if (rs.next()) {
                // Fixed column index from 0 to 1 (JDBC uses 1-based indexing)
                return rs.getInt(1);
            } else {
                return -1;
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public List<Integer> getAllUserIds() {
        List<Integer> list = new ArrayList<>();
        String sql = "SELECT id FROM users";
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                list.add(rs.getInt(1));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return list;
    }
}

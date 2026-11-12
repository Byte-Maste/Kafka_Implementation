import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Persistence for the two preserved output tables:
 *
 * <ul>
 *   <li>{@code messageprocess} - successfully processed messages</li>
 *   <li>{@code mcstats} - PRODUCED / CONSUMED metrics</li>
 * </ul>
 *
 * A third, additive table ({@code kafka_processed_offsets}) makes the
 * Kafka-to-database handoff idempotent. It is inserted in the same transaction
 * as the message row, so a crash between the database commit and the Kafka
 * offset commit cannot produce a duplicate {@code messageprocess} row.
 */
public class MessageProcessDAO {

    private static final Logger logger = Logger.getLogger(MessageProcessDAO.class.getName());

    private static final String CREATE_DEDUPE_TABLE = """
            CREATE TABLE IF NOT EXISTS kafka_processed_offsets (
                topic VARCHAR(20) NOT NULL,
                partition_no INT NOT NULL,
                record_offset BIGINT NOT NULL,
                processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (topic, partition_no, record_offset)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """;

    public void ensureSchema() {
        try (Connection connection = Database.getConnection();
             PreparedStatement statement = connection.prepareStatement(CREATE_DEDUPE_TABLE)) {
            statement.executeUpdate();
            logger.info("kafka_processed_offsets table is ready");
        } catch (SQLException e) {
            throw new RuntimeException("Unable to create kafka_processed_offsets table", e);
        }
    }

    public void recordStat(int msgId, String subTable, int userId, String actionType,
                           long waitTimeMs, long processingTimeMs) {
        String sql = "INSERT INTO mcstats(msg_id, sub_table, user_id, action_type, "
                + "wait_time_ms, processing_time_ms) VALUES (?,?,?,?,?,?)";
        try (Connection connection = Database.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, msgId);
            statement.setString(2, subTable);
            statement.setInt(3, userId);
            statement.setString(4, actionType);
            statement.setLong(5, waitTimeMs);
            statement.setLong(6, processingTimeMs);
            statement.executeUpdate();
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Failed to record " + actionType + " stat for msg " + msgId, e);
        }
    }

    /**
     * Saves one processed record and its CONSUMED stat in a single transaction.
     *
     * @return true when the row was inserted, false when this topic/partition/offset
     *         was already processed by an earlier attempt.
     */
    public boolean saveProcessed(QueuedMessage message) {
        long startTime = System.currentTimeMillis();
        Connection connection = null;
        try {
            connection = Database.getConnection();
            connection.setAutoCommit(false);

            if (!markOffsetProcessed(connection, message)) {
                connection.rollback();
                return false;
            }

            insertMessageProcess(connection, message);

            long processingTimeMs = System.currentTimeMillis() - startTime;
            insertConsumedStat(connection, message, processingTimeMs);

            connection.commit();
            return true;
        } catch (SQLException e) {
            rollbackQuietly(connection);
            throw new RuntimeException("Failed to save processed message at "
                    + message.subTable() + " offset " + message.offset(), e);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    logger.log(Level.WARNING, "Failed to close messageProcess connection", e);
                }
            }
        }
    }

    private boolean markOffsetProcessed(Connection connection, QueuedMessage message) throws SQLException {
        String sql = "INSERT IGNORE INTO kafka_processed_offsets(topic, partition_no, record_offset) "
                + "VALUES (?,?,?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, message.topic());
            statement.setInt(2, message.partition());
            statement.setLong(3, message.offset());
            return statement.executeUpdate() != 0;
        }
    }

    private void insertMessageProcess(Connection connection, QueuedMessage message) throws SQLException {
        String sql = "INSERT INTO messageprocess(msg_id, user_id, Content, Type, priority, isPayload, path) "
                + "VALUES (?,?,?,?,?,?,?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, message.msgId());
            statement.setInt(2, message.userId());
            statement.setString(3, message.content());
            statement.setString(4, message.type());
            statement.setBoolean(5, message.priority());
            statement.setBoolean(6, message.payload());
            statement.setString(7, message.path());
            statement.executeUpdate();
        }
    }

    private void insertConsumedStat(Connection connection, QueuedMessage message,
                                    long processingTimeMs) throws SQLException {
        String sql = "INSERT INTO mcstats(msg_id, sub_table, user_id, action_type, "
                + "wait_time_ms, processing_time_ms) VALUES (?,?,?,?,?,?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, message.msgId());
            statement.setString(2, message.subTable());
            statement.setInt(3, message.userId());
            statement.setString(4, "CONSUMED");
            statement.setLong(5, message.waitTimeMs());
            statement.setLong(6, processingTimeMs);
            statement.executeUpdate();
        }
    }

    private void rollbackQuietly(Connection connection) {
        if (connection == null) {
            return;
        }
        try {
            connection.rollback();
        } catch (SQLException e) {
            logger.log(Level.WARNING, "Failed to roll back message transaction", e);
        }
    }
}

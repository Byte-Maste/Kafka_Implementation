import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Central configuration loaded from {@code config.properties} on the classpath.
 * Every value can be overridden with a matching JVM system property, for example:
 * {@code -Dmq.storage.file-dir=C:/temp/messages}.
 */
public final class AppConfig {

    private static final String CONFIG_RESOURCE = "config.properties";
    private static final AppConfig INSTANCE = new AppConfig();

    private final Properties properties;

    private AppConfig() {
        properties = new Properties();
        try (InputStream input = AppConfig.class.getClassLoader().getResourceAsStream(CONFIG_RESOURCE)) {
            if (input == null) {
                throw new IllegalStateException("Missing " + CONFIG_RESOURCE + " on the classpath");
            }
            properties.load(input);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to load " + CONFIG_RESOURCE, e);
        }
    }

    public static AppConfig getInstance() {
        return INSTANCE;
    }

    public String get(String key) {
        String systemValue = System.getProperty(key);
        if (systemValue != null && !systemValue.isBlank()) {
            return systemValue.trim();
        }
        String value = properties.getProperty(key);
        if (value == null) {
            throw new IllegalStateException("Missing required configuration key: " + key);
        }
        return value.trim();
    }

    public String get(String key, String defaultValue) {
        String systemValue = System.getProperty(key);
        if (systemValue != null && !systemValue.isBlank()) {
            return systemValue.trim();
        }
        String value = properties.getProperty(key);
        return value == null ? defaultValue : value.trim();
    }

    public int getInt(String key, int defaultValue) {
        String value = get(key, null);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return Integer.parseInt(value);
    }

    public long getLong(String key, long defaultValue) {
        String value = get(key, null);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return Long.parseLong(value);
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        String value = get(key, null);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return Boolean.parseBoolean(value);
    }

    /**
     * Returns the primary broker followed by the optional failover broker.
     * Kafka clients accept a comma-separated bootstrap list and try each address.
     */
    public String getBootstrapServers() {
        String primary = get("kafka.bootstrap-servers");
        String failover = get("kafka.bootstrap-servers.failover", "");
        if (failover.isBlank() || primary.contains(failover)) {
            return primary;
        }
        return primary + "," + failover;
    }

    public int getPartitionsPerTopic() {
        int partitions = getInt("kafka.partitions-per-topic", 5);
        if (partitions < 1) {
            throw new IllegalStateException("kafka.partitions-per-topic must be at least 1");
        }
        return partitions;
    }

    public int getReplicationFactor() {
        int replicationFactor = getInt("kafka.replication-factor", 2);
        return Math.max(1, replicationFactor);
    }

    public String getConsumerGroupId() {
        return get("kafka.consumer.group-id", "message-processing-group");
    }

    public int getMaxPollRecords() {
        int maxPollRecords = getInt("kafka.consumer.max-poll-records", 500);
        return Math.max(1, maxPollRecords);
    }

    public int getPollTimeoutMs() {
        int pollTimeoutMs = getInt("kafka.consumer.poll-timeout-ms", 500);
        return Math.max(50, pollTimeoutMs);
    }

    public long getProducerDeliveryTimeoutMs() {
        long deliveryTimeoutMs = getLong("kafka.producer.delivery-timeout-ms", 120000L);
        return Math.max(1000L, deliveryTimeoutMs);
    }

    public int getMaxAllowedBytes() {
        int maxAllowedBytes = getInt("mq.size.max-allowed", 102400);
        if (maxAllowedBytes <= 0) {
            throw new IllegalStateException("mq.size.max-allowed must be positive");
        }
        return maxAllowedBytes;
    }

    public int getOffloadThresholdBytes() {
        int offloadThresholdBytes = getInt("mq.size.file-offload-threshold", 20480);
        if (offloadThresholdBytes <= 0) {
            throw new IllegalStateException("mq.size.file-offload-threshold must be positive");
        }
        return offloadThresholdBytes;
    }

    public Path getFileStorageDir() {
        return Paths.get(get("mq.storage.file-dir", "./message-payloads/"))
                .toAbsolutePath()
                .normalize();
    }

    public long getSchedulerIntervalMinutes() {
        long interval = getLong("mq.scheduler.interval-minutes", 5L);
        return Math.max(1L, interval);
    }

    public long getSchedulerInitialDelayMinutes() {
        long initialDelay = getLong("mq.scheduler.initial-delay-minutes", 5L);
        return Math.max(0L, initialDelay);
    }

    public String getDbUrl() {
        return get("db.url", "jdbc:mysql://localhost:3306/message_queue");
    }

    public String getDbUsername() {
        return get("db.username", "root");
    }

    public String getDbPassword() {
        return get("db.password", "root");
    }

    public int getHttpPort() {
        int port = getInt("http.port", 8080);
        if (port < 1 || port > 65535) {
            throw new IllegalStateException("http.port must be between 1 and 65535");
        }
        return port;
    }
}

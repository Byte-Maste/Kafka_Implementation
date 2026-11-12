import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import java.util.logging.Logger;

/**
 * Stores oversized message content on the local filesystem. The Kafka record
 * then carries only {@code [FILE_REF]:<absolute path>}.
 */
public class FilePayloadManager {

    public static final String FILE_REF_PREFIX = "[FILE_REF]:";
    private static final int MAX_STORED_PATH_LENGTH = 512;
    private static final Logger logger = Logger.getLogger(FilePayloadManager.class.getName());

    private final Path storageDir;
    private final int maxAllowedBytes;
    private final int offloadThresholdBytes;

    public FilePayloadManager() {
        this(AppConfig.getInstance().getFileStorageDir(),
                AppConfig.getInstance().getMaxAllowedBytes(),
                AppConfig.getInstance().getOffloadThresholdBytes());
    }

    FilePayloadManager(Path storageDir, int maxAllowedBytes, int offloadThresholdBytes) {
        this.storageDir = storageDir.toAbsolutePath().normalize();
        this.maxAllowedBytes = maxAllowedBytes;
        this.offloadThresholdBytes = offloadThresholdBytes;
    }

    /**
     * @return {@code null} when the content fits inline, otherwise the absolute
     *         path of the stored payload file.
     * @throws IllegalArgumentException when the UTF-8 content exceeds the limit.
     */
    public String savePayload(String content) throws IOException {
        if (content == null) {
            return null;
        }

        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > maxAllowedBytes) {
            logger.warning("Rejected message payload of " + bytes.length + " bytes");
            throw new IllegalArgumentException("Size limit reached");
        }
        if (bytes.length <= offloadThresholdBytes) {
            return null;
        }

        Files.createDirectories(storageDir);
        Path payloadPath = storageDir.resolve("payload-" + UUID.randomUUID() + ".dat").normalize();
        if (payloadPath.toString().length() > MAX_STORED_PATH_LENGTH) {
            throw new IOException("Payload path exceeds the messageProcess.path limit of "
                    + MAX_STORED_PATH_LENGTH + " characters: " + payloadPath);
        }

        Files.write(payloadPath, bytes, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        logger.info("Offloaded " + bytes.length + " byte payload to " + payloadPath);
        return payloadPath.toString();
    }

    public String readPayload(String filePath) throws IOException {
        if (filePath == null || filePath.isBlank()) {
            throw new IllegalArgumentException("Payload path is required");
        }
        return Files.readString(Paths.get(filePath), StandardCharsets.UTF_8);
    }

    public static boolean isFileReference(String value) {
        return value != null && value.startsWith(FILE_REF_PREFIX);
    }

    public static String extractPath(String value) {
        if (!isFileReference(value)) {
            return null;
        }
        return value.substring(FILE_REF_PREFIX.length());
    }

    public Path getStorageDir() {
        return storageDir;
    }

    public int getMaxAllowedBytes() {
        return maxAllowedBytes;
    }

    public int getOffloadThresholdBytes() {
        return offloadThresholdBytes;
    }
}

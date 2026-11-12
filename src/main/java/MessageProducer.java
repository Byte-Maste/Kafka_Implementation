import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Durable Kafka producer for the portal and the scheduled sample poster.
 *
 * <p>The message key is the userId, so Kafka's default partitioner keeps every
 * user's messages on the same partition (the equivalent of the old sticky
 * sub-table routing).</p>
 */
public class MessageProducer {

    private static final Logger logger = Logger.getLogger(MessageProducer.class.getName());
    private static final int SEND_TIMEOUT_SECONDS = 30;

    private final AppConfig config;
    private final FilePayloadManager filePayloadManager;
    private final MessageProcessDAO messageProcessDAO;
    private final KafkaProducer<String, String> producer;
    private final AtomicInteger msgIdGenerator;

    public MessageProducer(AppConfig config, MessageProcessDAO messageProcessDAO) {
        this.config = config;
        this.messageProcessDAO = messageProcessDAO;
        this.filePayloadManager = new FilePayloadManager();
        this.producer = KafkaClientFactory.createProducer(config);
        this.msgIdGenerator = new AtomicInteger(
                (int) (System.currentTimeMillis() % 1_000_000_000L) + 1);
    }

    public SendResult send(int userId, String type, String content, boolean priority) {
        if (userId <= 0) {
            throw new IllegalArgumentException("A valid userId is required");
        }
        if (content == null) {
            throw new IllegalArgumentException("Message content is required");
        }

        MessageType messageType = MessageType.fromInput(type);

        String filePath;
        try {
            filePath = filePayloadManager.savePayload(content);
        } catch (IOException e) {
            throw new RuntimeException("Unable to store oversized message payload", e);
        }

        boolean isPayload = filePath != null;
        String kafkaValue = isPayload ? FilePayloadManager.FILE_REF_PREFIX + filePath : content;
        int msgId = nextMsgId();
        long producedAt = System.currentTimeMillis();

        ProducerRecord<String, String> record = new ProducerRecord<>(
                messageType.topic(),
                String.valueOf(userId),
                kafkaValue);
        addHeader(record, "isPriority", String.valueOf(priority));
        addHeader(record, "userId", String.valueOf(userId));
        addHeader(record, "msgId", String.valueOf(msgId));
        addHeader(record, "producedAt", String.valueOf(producedAt));
        addHeader(record, "messageType", messageType.name());
        addHeader(record, "isPayload", String.valueOf(isPayload));

        long sendStart = System.currentTimeMillis();
        try {
            RecordMetadata metadata = producer.send(record).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            long sendTimeMs = System.currentTimeMillis() - sendStart;

            messageProcessDAO.recordStat(
                    msgId,
                    messageType.topic() + "-" + metadata.partition(),
                    userId,
                    "PRODUCED",
                    0L,
                    sendTimeMs);

            logger.info("Published msgId=" + msgId + " userId=" + userId
                    + " type=" + messageType + " partition=" + metadata.partition()
                    + " offset=" + metadata.offset() + " payload=" + isPayload);
            return new SendResult(metadata.topic(), metadata.partition(), metadata.offset(), msgId, isPayload);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while publishing message", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new RuntimeException("Failed to publish message to Kafka", e);
        }
    }

    private int nextMsgId() {
        int next = msgIdGenerator.updateAndGet(current -> current == Integer.MAX_VALUE ? 1 : current + 1);
        return next > 0 ? next : 1;
    }

    private static void addHeader(ProducerRecord<String, String> record, String name, String value) {
        record.headers().add(name, value.getBytes(StandardCharsets.UTF_8));
    }

    public void close() {
        try {
            producer.flush();
        } catch (RuntimeException e) {
            logger.log(Level.WARNING, "Producer flush failed during shutdown", e);
        } finally {
            producer.close(java.time.Duration.ofSeconds(10));
        }
    }

    public record SendResult(String topic, int partition, long offset, int msgId, boolean payload) {
    }
}

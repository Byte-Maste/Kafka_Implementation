import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.errors.WakeupException;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * One dedicated worker per Kafka partition. The worker uses explicit
 * {@code assign(...)} rather than group subscription, so the partition-to-thread
 * mapping is always 1:1. Offsets are committed manually only after every record
 * in the polled batch has been written to the database.
 */
public class PartitionConsumerWorker implements Runnable {

    private static final Logger logger = Logger.getLogger(PartitionConsumerWorker.class.getName());
    private static final long RETRY_BACKOFF_MS = 2000L;

    private final AppConfig config;
    private final TopicPartition topicPartition;
    private final MessageProcessDAO messageProcessDAO;

    private volatile boolean running = true;
    private volatile KafkaConsumer<String, String> consumer;

    public PartitionConsumerWorker(AppConfig config,
                                   TopicPartition topicPartition,
                                   MessageProcessDAO messageProcessDAO) {
        this.config = config;
        this.topicPartition = topicPartition;
        this.messageProcessDAO = messageProcessDAO;
    }

    @Override
    public void run() {
        logger.info("Starting partition worker for " + topicPartition);
        while (running) {
            try {
                consumer = KafkaClientFactory.createConsumer(config, topicPartition);
                consumer.assign(List.of(topicPartition));
                logger.info("Assigned " + topicPartition + " to " + Thread.currentThread().getName());
                pollLoop();
            } catch (WakeupException e) {
                if (running) {
                    logger.log(Level.WARNING, "Unexpected consumer wakeup for " + topicPartition, e);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.info("Partition worker interrupted for " + topicPartition);
                break;
            } catch (Exception e) {
                if (running) {
                    logger.log(Level.SEVERE, "Partition worker failed for " + topicPartition
                            + "; recreating consumer", e);
                    try {
                        sleepRetryBackoff();
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            } finally {
                closeConsumer();
            }
        }
        logger.info("Stopped partition worker for " + topicPartition);
    }

    private void pollLoop() throws InterruptedException {
        while (running) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(config.getPollTimeoutMs()));
            if (records.isEmpty()) {
                continue;
            }

            List<ConsumerRecord<String, String>> partitionRecords = records.records(topicPartition);
            if (partitionRecords.isEmpty()) {
                continue;
            }

            long firstOffset = partitionRecords.get(0).offset();
            long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();

            List<QueuedMessage> parsed = new ArrayList<>(partitionRecords.size());
            for (ConsumerRecord<String, String> record : partitionRecords) {
                QueuedMessage message = parse(record);
                if (message != null) {
                    parsed.add(message);
                }
            }

            try {
                List<QueuedMessage> ordered = PriorityBatchPlanner.plan(parsed);
                for (QueuedMessage message : ordered) {
                    boolean inserted = messageProcessDAO.saveProcessed(message);
                    if (inserted) {
                        logger.info("Processed " + message.subTable() + " offset=" + message.offset()
                                + " msgId=" + message.msgId() + " userId=" + message.userId()
                                + " type=" + message.type() + " priority=" + message.priority()
                                + " payload=" + message.payload());
                    } else {
                        logger.info("Skipped duplicate " + message.subTable()
                                + " offset=" + message.offset() + " msgId=" + message.msgId());
                    }
                }

                commitOffset(lastOffset + 1);
            } catch (RuntimeException e) {
                if (!running) {
                    throw e;
                }
                logger.log(Level.SEVERE, "Batch failed for " + topicPartition
                        + " offsets " + firstOffset + ".." + lastOffset
                        + "; seeking back and retrying", e);
                consumer.seek(topicPartition, firstOffset);
                sleepRetryBackoff();
            }
        }
    }

    private void commitOffset(long nextOffset) {
        Map<TopicPartition, OffsetAndMetadata> offsets =
                Map.of(topicPartition, new OffsetAndMetadata(nextOffset));
        consumer.commitSync(offsets, Duration.ofSeconds(10));
        logger.fine("Committed " + topicPartition + " at offset " + nextOffset);
    }

    private QueuedMessage parse(ConsumerRecord<String, String> record) {
        String value = record.value();
        if (value == null) {
            logger.warning("Skipping null Kafka value at " + record.topic()
                    + "-" + record.partition() + " offset " + record.offset());
            return null;
        }

        Integer headerUserId = parseInt(header(record, "userId"));
        Integer keyUserId = parseInt(record.key());
        Integer userId = headerUserId != null ? headerUserId : keyUserId;
        if (userId == null || userId <= 0) {
            logger.warning("Skipping record without a valid userId at " + record.topic()
                    + "-" + record.partition() + " offset " + record.offset());
            return null;
        }

        MessageType messageType;
        try {
            messageType = MessageType.fromTopic(record.topic());
        } catch (IllegalArgumentException e) {
            logger.warning("Skipping record from unsupported topic " + record.topic());
            return null;
        }

        boolean priority = parseBoolean(header(record, "isPriority"));
        boolean payload = parseBoolean(header(record, "isPayload"))
                || FilePayloadManager.isFileReference(value);

        String content = value;
        String path = null;
        if (payload) {
            path = FilePayloadManager.extractPath(value);
            if (path == null || path.isBlank()) {
                logger.warning("Skipping invalid file reference at " + record.topic()
                        + "-" + record.partition() + " offset " + record.offset());
                return null;
            }
            content = null;
        }

        Integer headerMsgId = parseInt(header(record, "msgId"));
        int msgId = headerMsgId != null ? headerMsgId : fallbackMsgId(record.offset());

        Long headerProducedAt = parseLong(header(record, "producedAt"));
        long producedAt = headerProducedAt != null ? headerProducedAt : record.timestamp();
        if (producedAt <= 0) {
            producedAt = System.currentTimeMillis();
        }

        return new QueuedMessage(
                record.topic(),
                record.partition(),
                record.offset(),
                userId,
                content,
                messageType.name(),
                priority,
                payload,
                path,
                producedAt,
                msgId);
    }

    private static int fallbackMsgId(long offset) {
        int value = (int) (offset & 0x7FFFFFFFL);
        return value == 0 ? 1 : value;
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        if (header == null || header.value() == null) {
            return null;
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }

    private static boolean parseBoolean(String value) {
        if (value == null) {
            return false;
        }
        return value.equalsIgnoreCase("true")
                || value.equalsIgnoreCase("yes")
                || value.equalsIgnoreCase("1");
    }

    private static Integer parseInt(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Long parseLong(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private void sleepRetryBackoff() throws InterruptedException {
        long remaining = RETRY_BACKOFF_MS;
        while (running && remaining > 0) {
            long chunk = Math.min(200L, remaining);
            Thread.sleep(chunk);
            remaining -= chunk;
        }
    }

    private void closeConsumer() {
        KafkaConsumer<String, String> current = consumer;
        consumer = null;
        if (current == null) {
            return;
        }
        try {
            current.close(Duration.ofSeconds(5));
        } catch (RuntimeException e) {
            logger.log(Level.WARNING, "Failed to close consumer for " + topicPartition, e);
        }
    }

    public void stop() {
        running = false;
        KafkaConsumer<String, String> current = consumer;
        if (current != null) {
            try {
                current.wakeup();
            } catch (IllegalStateException e) {
                logger.log(Level.FINE, "Consumer already closed for " + topicPartition, e);
            }
        }
    }

    public boolean isRunning() {
        return running;
    }

    public TopicPartition getTopicPartition() {
        return topicPartition;
    }
}

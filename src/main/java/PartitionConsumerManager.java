import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Creates the four message topics (when necessary) and starts one dedicated
 * consumer thread for every partition of every topic.
 */
public class PartitionConsumerManager {

    private static final Logger logger = Logger.getLogger(PartitionConsumerManager.class.getName());
    private static final int MAX_TOPIC_ATTEMPTS = 12;
    private static final long TOPIC_RETRY_DELAY_MS = 5000L;

    private final AppConfig config;
    private final MessageProcessDAO messageProcessDAO;
    private final List<PartitionConsumerWorker> workers = new ArrayList<>();
    private final Map<TopicPartition, Thread> workerThreads = new LinkedHashMap<>();

    private volatile boolean running;

    public PartitionConsumerManager(AppConfig config, MessageProcessDAO messageProcessDAO) {
        this.config = config;
        this.messageProcessDAO = messageProcessDAO;
    }

    public synchronized void start() {
        if (running) {
            return;
        }

        List<TopicPartition> partitions = ensureTopicsWithRetry();
        if (partitions.isEmpty()) {
            throw new IllegalStateException("No Kafka partitions are available for the message topics");
        }

        running = true;
        for (TopicPartition topicPartition : partitions) {
            PartitionConsumerWorker worker =
                    new PartitionConsumerWorker(config, topicPartition, messageProcessDAO);
            Thread thread = new Thread(worker,
                    "kafka-consumer-" + topicPartition.topic() + "-" + topicPartition.partition());
            thread.setDaemon(false);
            workers.add(worker);
            workerThreads.put(topicPartition, thread);
            thread.start();
        }

        logger.info("Started " + workers.size() + " Kafka partition workers across "
                + MessageType.values().length + " topics");
    }

    private List<TopicPartition> ensureTopicsWithRetry() {
        RuntimeException lastFailure = null;
        for (int attempt = 1; attempt <= MAX_TOPIC_ATTEMPTS; attempt++) {
            try {
                return ensureTopics();
            } catch (Exception e) {
                lastFailure = new RuntimeException("Unable to prepare Kafka topics on attempt "
                        + attempt + "/" + MAX_TOPIC_ATTEMPTS, e);
                logger.log(Level.WARNING, "Kafka topic preparation failed on attempt "
                        + attempt + "/" + MAX_TOPIC_ATTEMPTS, e);
                if (attempt < MAX_TOPIC_ATTEMPTS) {
                    try {
                        Thread.sleep(TOPIC_RETRY_DELAY_MS);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted while waiting for Kafka", interrupted);
                    }
                }
            }
        }
        throw lastFailure == null
                ? new IllegalStateException("Unable to prepare Kafka topics")
                : lastFailure;
    }

    private List<TopicPartition> ensureTopics() throws Exception {
        try (AdminClient admin = KafkaClientFactory.createAdmin(config)) {
            Set<String> existingTopics = admin.listTopics().names().get(30, TimeUnit.SECONDS);
            int brokerCount = admin.describeCluster().nodes().get(30, TimeUnit.SECONDS).size();
            short replicationFactor = (short) Math.min(config.getReplicationFactor(), Math.max(1, brokerCount));

            for (MessageType type : MessageType.values()) {
                if (existingTopics.contains(type.topic())) {
                    continue;
                }
                NewTopic newTopic = new NewTopic(type.topic(), config.getPartitionsPerTopic(), replicationFactor);
                try {
                    admin.createTopics(List.of(newTopic)).all().get(30, TimeUnit.SECONDS);
                    logger.info("Created topic " + type.topic() + " with "
                            + config.getPartitionsPerTopic() + " partitions and replication factor "
                            + replicationFactor);
                } catch (ExecutionException e) {
                    if (!(e.getCause() instanceof TopicExistsException)) {
                        throw e;
                    }
                    logger.info("Topic " + type.topic() + " was created concurrently");
                }
            }

            List<String> topicNames = Arrays.stream(MessageType.values())
                    .map(MessageType::topic)
                    .toList();
            Map<String, TopicDescription> descriptions =
                    admin.describeTopics(topicNames).allTopicNames().get(30, TimeUnit.SECONDS);

            List<TopicPartition> partitions = new ArrayList<>();
            for (MessageType type : MessageType.values()) {
                TopicDescription description = descriptions.get(type.topic());
                if (description == null) {
                    throw new IllegalStateException("Kafka did not return metadata for topic " + type.topic());
                }
                int actualPartitions = description.partitions().size();
                if (actualPartitions != config.getPartitionsPerTopic()) {
                    logger.warning("Topic " + type.topic() + " has " + actualPartitions
                            + " partitions, but kafka.partitions-per-topic="
                            + config.getPartitionsPerTopic() + "; using the existing partition count");
                }
                for (int partition = 0; partition < actualPartitions; partition++) {
                    partitions.add(new TopicPartition(type.topic(), partition));
                }
            }
            return partitions;
        }
    }

    public synchronized void stop() {
        if (!running && workers.isEmpty()) {
            return;
        }
        running = false;

        for (PartitionConsumerWorker worker : workers) {
            worker.stop();
        }

        for (Map.Entry<TopicPartition, Thread> entry : workerThreads.entrySet()) {
            Thread thread = entry.getValue();
            try {
                thread.join(5000);
                if (thread.isAlive()) {
                    logger.warning("Worker " + entry.getKey() + " did not stop in 5 seconds; interrupting");
                    thread.interrupt();
                    thread.join(2000);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.log(Level.WARNING, "Interrupted while stopping worker " + entry.getKey(), e);
                break;
            }
        }

        workers.clear();
        workerThreads.clear();
        logger.info("Stopped all Kafka partition workers");
    }

    public int getWorkerCount() {
        return workers.size();
    }

    public boolean isRunning() {
        return running;
    }
}

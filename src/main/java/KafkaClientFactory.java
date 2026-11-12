import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Builds Kafka clients with the durability and commit settings required by the
 * message pipeline.
 */
public final class KafkaClientFactory {

    private KafkaClientFactory() {
    }

    public static Properties producerProperties(AppConfig config) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, (int) config.getProducerDeliveryTimeoutMs());
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "message-producer");
        return properties;
    }

    public static KafkaProducer<String, String> createProducer(AppConfig config) {
        return new KafkaProducer<>(producerProperties(config));
    }

    public static Properties consumerProperties(AppConfig config, TopicPartition topicPartition) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, config.getConsumerGroupId());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, config.getMaxPollRecords());
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
        properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        properties.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 30000);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG,
                "partition-consumer-" + topicPartition.topic() + "-" + topicPartition.partition());
        return properties;
    }

    public static KafkaConsumer<String, String> createConsumer(AppConfig config, TopicPartition topicPartition) {
        return new KafkaConsumer<>(consumerProperties(config, topicPartition));
    }

    public static Properties adminProperties(AppConfig config) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        properties.put(AdminClientConfig.CLIENT_ID_CONFIG, "message-admin");
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 30000);
        return properties;
    }

    public static AdminClient createAdmin(AppConfig config) {
        return AdminClient.create(adminProperties(config));
    }
}

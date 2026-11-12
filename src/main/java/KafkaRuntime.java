import java.util.logging.Logger;

/**
 * Owns the producer, partition workers, and scheduler for the lifetime of the
 * application. Both Main (embedded Tomcat) and ListenerClass (external Tomcat)
 * call the same idempotent lifecycle.
 */
public final class KafkaRuntime {

    private static final Logger logger = Logger.getLogger(KafkaRuntime.class.getName());
    private static final KafkaRuntime INSTANCE = new KafkaRuntime();

    private final Object lock = new Object();

    private MessageProcessDAO messageProcessDAO;
    private MessageProducer messageProducer;
    private PartitionConsumerManager partitionConsumerManager;
    private MessageScheduler messageScheduler;
    private volatile boolean started;

    private KafkaRuntime() {
    }

    public static KafkaRuntime getInstance() {
        return INSTANCE;
    }

    public void start() {
        synchronized (lock) {
            if (started) {
                return;
            }

            AppConfig config = AppConfig.getInstance();
            MessageProcessDAO newDAO = new MessageProcessDAO();
            newDAO.ensureSchema();

            MessageProducer newProducer = new MessageProducer(config, newDAO);
            PartitionConsumerManager newManager = new PartitionConsumerManager(config, newDAO);
            MessageScheduler newScheduler = new MessageScheduler(newProducer, new UserDAO(), config);

            try {
                newManager.start();
                newScheduler.start();
            } catch (RuntimeException e) {
                newScheduler.stop();
                newManager.stop();
                newProducer.close();
                throw e;
            }

            this.messageProcessDAO = newDAO;
            this.messageProducer = newProducer;
            this.partitionConsumerManager = newManager;
            this.messageScheduler = newScheduler;
            this.started = true;
            logger.info("Kafka message runtime started");
        }
    }

    public void stop() {
        synchronized (lock) {
            if (!started) {
                return;
            }
            started = false;

            if (messageScheduler != null) {
                messageScheduler.stop();
            }
            if (partitionConsumerManager != null) {
                partitionConsumerManager.stop();
            }
            if (messageProducer != null) {
                messageProducer.close();
            }

            messageScheduler = null;
            partitionConsumerManager = null;
            messageProducer = null;
            messageProcessDAO = null;
            logger.info("Kafka message runtime stopped");
        }
    }

    public MessageProducer getProducer() {
        synchronized (lock) {
            if (messageProducer == null) {
                throw new IllegalStateException("Kafka runtime has not been started");
            }
            return messageProducer;
        }
    }

    public boolean isStarted() {
        return started;
    }
}

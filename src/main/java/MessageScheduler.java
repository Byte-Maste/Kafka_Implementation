import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Posts one non-priority sample message for every existing user and every
 * message type on a fixed five-minute schedule.
 */
public class MessageScheduler {

    private static final Logger logger = Logger.getLogger(MessageScheduler.class.getName());

    private final MessageProducer messageProducer;
    private final UserDAO userDAO;
    private final AppConfig config;

    private ScheduledExecutorService executor;
    private volatile boolean running;

    public MessageScheduler(MessageProducer messageProducer, UserDAO userDAO, AppConfig config) {
        this.messageProducer = messageProducer;
        this.userDAO = userDAO;
        this.config = config;
    }

    public synchronized void start() {
        if (running) {
            return;
        }

        long intervalMinutes = config.getSchedulerIntervalMinutes();
        long initialDelayMinutes = config.getSchedulerInitialDelayMinutes();
        executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "message-scheduler");
            thread.setDaemon(false);
            return thread;
        });
        // Fixed delay rather than fixed rate: after a laptop sleep or long GC
        // pause, scheduleAtFixedRate replays every missed period back-to-back.
        // Fixed delay always waits a full interval after the previous run.
        executor.scheduleWithFixedDelay(this::runOnce,
                initialDelayMinutes,
                intervalMinutes,
                TimeUnit.MINUTES);
        running = true;
        logger.info("Message scheduler started: initial delay=" + initialDelayMinutes
                + " minute(s), interval=" + intervalMinutes + " minute(s)");
    }

    /**
     * Public so the schedule can also be triggered manually during verification.
     */
    public void runOnce() {
        try {
            List<Integer> userIds = userDAO.getAllUserIds();
            if (userIds.isEmpty()) {
                logger.info("Scheduled sample posting skipped because there are no users");
                return;
            }

            int sent = 0;
            String timestamp = Instant.now().toString();
            for (Integer userId : userIds) {
                for (MessageType type : MessageType.values()) {
                    String content = "Scheduled " + type.name() + " sample for user "
                            + userId + " at " + timestamp;
                    try {
                        messageProducer.send(userId, type.name(), content, false);
                        sent++;
                    } catch (RuntimeException e) {
                        logger.log(Level.WARNING, "Scheduled sample failed for user " + userId
                                + " type " + type, e);
                    }
                }
            }
            logger.info("Scheduled sample posting completed: " + sent + " message(s)");
        } catch (RuntimeException e) {
            logger.log(Level.SEVERE, "Scheduled sample posting failed", e);
        }
    }

    public synchronized void stop() {
        if (!running && executor == null) {
            return;
        }
        running = false;
        if (executor != null) {
            executor.shutdownNow();
            try {
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    logger.warning("Message scheduler did not terminate within 5 seconds");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.log(Level.WARNING, "Interrupted while stopping message scheduler", e);
            }
            executor = null;
        }
        logger.info("Message scheduler stopped");
    }

    public boolean isRunning() {
        return running;
    }
}

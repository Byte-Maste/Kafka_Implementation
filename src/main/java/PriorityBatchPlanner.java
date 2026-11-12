import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Reorders one polled batch so that priority messages follow the same rule as
 * the old database consumer:
 *
 * <ol>
 *   <li>Pick the oldest priority record across all users.</li>
 *   <li>Drain that user's older regular records (the backlog) in FIFO order.</li>
 *   <li>Process the priority record.</li>
 *   <li>Repeat for the next oldest priority record.</li>
 *   <li>Finally drain all remaining regular records in offset order.</li>
 * </ol>
 *
 * The planner is intentionally bounded to the batch supplied by
 * {@code KafkaConsumer.poll(...)}. It never buffers an unbounded backlog.
 */
public final class PriorityBatchPlanner {

    private PriorityBatchPlanner() {
    }

    public static List<QueuedMessage> plan(List<QueuedMessage> batch) {
        if (batch == null || batch.isEmpty()) {
            return List.of();
        }

        Map<Integer, UserQueues> users = new ConcurrentHashMap<>();
        PriorityQueue<QueuedMessage> priorities =
                new PriorityQueue<>(Comparator.comparingLong(QueuedMessage::offset));

        for (QueuedMessage message : batch) {
            UserQueues queues = users.computeIfAbsent(message.userId(), ignored -> new UserQueues());
            if (message.priority()) {
                queues.priority.add(message);
                priorities.add(message);
            } else {
                queues.regular.add(message);
            }
        }

        List<QueuedMessage> ordered = new ArrayList<>(batch.size());

        while (!priorities.isEmpty()) {
            QueuedMessage priority = priorities.poll();
            UserQueues queues = users.get(priority.userId());
            drainRegularBefore(queues.regular, priority.offset(), ordered);
            ordered.add(priority);
        }

        List<QueuedMessage> remainingRegular = new ArrayList<>();
        for (UserQueues queues : users.values()) {
            QueuedMessage message;
            while ((message = queues.regular.poll()) != null) {
                remainingRegular.add(message);
            }
        }
        remainingRegular.sort(Comparator.comparingLong(QueuedMessage::offset));
        ordered.addAll(remainingRegular);

        return ordered;
    }

    private static void drainRegularBefore(ConcurrentLinkedQueue<QueuedMessage> regular,
                                           long priorityOffset,
                                           List<QueuedMessage> ordered) {
        while (true) {
            QueuedMessage head = regular.peek();
            if (head == null || head.offset() >= priorityOffset) {
                return;
            }
            ordered.add(regular.poll());
        }
    }

    private static final class UserQueues {
        private final ConcurrentLinkedQueue<QueuedMessage> regular = new ConcurrentLinkedQueue<>();
        private final ConcurrentLinkedQueue<QueuedMessage> priority = new ConcurrentLinkedQueue<>();
    }
}

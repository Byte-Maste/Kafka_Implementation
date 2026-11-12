import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PriorityBatchPlannerTest {

    @Test
    void regularMessagesRemainInOffsetOrder() {
        List<QueuedMessage> batch = List.of(
                message(0, 1, false),
                message(1, 2, false),
                message(2, 1, false));

        assertEquals(List.of(0L, 1L, 2L), offsets(PriorityBatchPlanner.plan(batch)));
    }

    @Test
    void priorityDrainsItsOwnOlderBacklogBeforeBeingProcessed() {
        List<QueuedMessage> batch = List.of(
                message(0, 1, false),
                message(1, 2, false),
                message(2, 1, false),
                message(3, 1, true),
                message(4, 1, false),
                message(5, 2, true),
                message(6, 2, false));

        // Oldest priority is offset 3 (user 1): drain 0, 2, then 3.
        // Next priority is offset 5 (user 2): drain 1, then 5.
        // Remaining regular messages are 4 and 6.
        assertEquals(List.of(0L, 2L, 3L, 1L, 5L, 4L, 6L),
                offsets(PriorityBatchPlanner.plan(batch)));
    }

    @Test
    void multiplePrioritiesForOneUserKeepBacklogOrdering() {
        List<QueuedMessage> batch = List.of(
                message(0, 1, false),
                message(1, 1, true),
                message(2, 1, false),
                message(3, 1, true),
                message(4, 1, false));

        assertEquals(List.of(0L, 1L, 2L, 3L, 4L),
                offsets(PriorityBatchPlanner.plan(batch)));
    }

    @Test
    void priorityAtTheFrontDoesNotConsumeLaterRegularMessages() {
        List<QueuedMessage> batch = List.of(
                message(0, 1, true),
                message(1, 1, false),
                message(2, 2, false));

        assertEquals(List.of(0L, 1L, 2L),
                offsets(PriorityBatchPlanner.plan(batch)));
    }

    @Test
    void emptyBatchProducesEmptyPlan() {
        assertTrue(PriorityBatchPlanner.plan(List.of()).isEmpty());
    }

    private static List<Long> offsets(List<QueuedMessage> messages) {
        return messages.stream().map(QueuedMessage::offset).toList();
    }

    private static QueuedMessage message(long offset, int userId, boolean priority) {
        return new QueuedMessage(
                "INFO",
                0,
                offset,
                userId,
                "content-" + offset,
                "INFO",
                priority,
                false,
                null,
                System.currentTimeMillis(),
                (int) offset + 1);
    }
}

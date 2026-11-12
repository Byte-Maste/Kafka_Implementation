/**
 * Immutable representation of one Kafka record after its headers have been
 * parsed. {@code content} is null when the payload was offloaded to disk.
 */
public record QueuedMessage(
        String topic,
        int partition,
        long offset,
        int userId,
        String content,
        String type,
        boolean priority,
        boolean payload,
        String path,
        long producedAt,
        int msgId) {

    public String subTable() {
        return topic + "-" + partition;
    }

    public long waitTimeMs() {
        return Math.max(0L, System.currentTimeMillis() - producedAt);
    }
}

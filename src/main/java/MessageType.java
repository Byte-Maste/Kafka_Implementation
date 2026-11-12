import java.util.Arrays;
import java.util.Locale;

/**
 * The four message types are also the Kafka topic names. The old database
 * sub-tables used lowercase names, so legacy inputs remain accepted.
 */
public enum MessageType {

    INFO,
    WARN,
    FINE,
    SEVERE;

    public String topic() {
        return name();
    }

    public static MessageType fromInput(String input) {
        if (input == null || input.isBlank()) {
            throw new IllegalArgumentException("Message type is required");
        }

        String normalized = input.trim().toUpperCase(Locale.ROOT);
        for (MessageType type : values()) {
            if (type.name().equals(normalized)) {
                return type;
            }
        }

        // Accept the T1-T4 aliases from the original brief as a convenience.
        return switch (normalized) {
            case "T1" -> INFO;
            case "T2" -> WARN;
            case "T3" -> FINE;
            case "T4" -> SEVERE;
            default -> throw new IllegalArgumentException("Unsupported message type: " + input);
        };
    }

    public static MessageType fromTopic(String topic) {
        if (topic == null) {
            throw new IllegalArgumentException("Topic is required");
        }
        return fromInput(topic);
    }

    public static String[] topicNames() {
        return Arrays.stream(values())
                .map(MessageType::topic)
                .toArray(String[]::new);
    }
}

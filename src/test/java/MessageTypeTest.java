import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MessageTypeTest {

    @Test
    void canonicalNamesMapToTopics() {
        assertEquals("INFO", MessageType.INFO.topic());
        assertEquals("WARN", MessageType.WARN.topic());
        assertEquals("FINE", MessageType.FINE.topic());
        assertEquals("SEVERE", MessageType.SEVERE.topic());
    }

    @Test
    void inputIsCaseInsensitive() {
        assertEquals(MessageType.INFO, MessageType.fromInput("info"));
        assertEquals(MessageType.WARN, MessageType.fromInput(" Warn "));
        assertEquals(MessageType.FINE, MessageType.fromInput("fine"));
        assertEquals(MessageType.SEVERE, MessageType.fromInput("SEVERE"));
    }

    @Test
    void legacyTopicAliasesAreAccepted() {
        assertEquals(MessageType.INFO, MessageType.fromInput("T1"));
        assertEquals(MessageType.WARN, MessageType.fromInput("T2"));
        assertEquals(MessageType.FINE, MessageType.fromInput("T3"));
        assertEquals(MessageType.SEVERE, MessageType.fromInput("T4"));
    }

    @Test
    void topicsMapBackToTypes() {
        assertEquals(MessageType.INFO, MessageType.fromTopic("INFO"));
        assertEquals(MessageType.WARN, MessageType.fromTopic("warn"));
    }

    @Test
    void invalidInputIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> MessageType.fromInput("DEBUG"));
        assertThrows(IllegalArgumentException.class, () -> MessageType.fromInput(null));
        assertThrows(IllegalArgumentException.class, () -> MessageType.fromInput(" "));
    }

    @Test
    void topicNamesAreInDeclarationOrder() {
        assertArrayEquals(new String[]{"INFO", "WARN", "FINE", "SEVERE"}, MessageType.topicNames());
    }
}

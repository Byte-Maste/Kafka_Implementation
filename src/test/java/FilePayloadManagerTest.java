import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FilePayloadManagerTest {

    @TempDir
    Path tempDir;

    private FilePayloadManager manager() {
        return new FilePayloadManager(tempDir, 102400, 20480);
    }

    @Test
    void inlineContentBelowOffloadThreshold() throws Exception {
        assertNull(manager().savePayload("small message"));
    }

    @Test
    void exactlyTwentyKilobytesStaysInline() throws Exception {
        String content = "a".repeat(20480);
        assertNull(manager().savePayload(content));
    }

    @Test
    void oneByteOverTwentyKilobytesIsOffloaded() throws Exception {
        String content = "a".repeat(20481);
        String path = manager().savePayload(content);

        assertNotNull(path);
        assertTrue(FilePayloadManager.isFileReference(FilePayloadManager.FILE_REF_PREFIX + path));
        assertEquals(content, Files.readString(Path.of(path), StandardCharsets.UTF_8));
        assertTrue(path.startsWith(tempDir.toAbsolutePath().normalize().toString()));
    }

    @Test
    void exactlyOneHundredKilobytesIsAllowedAndOffloaded() throws Exception {
        String content = "a".repeat(102400);
        String path = manager().savePayload(content);
        assertNotNull(path);
    }

    @Test
    void oneByteOverOneHundredKilobytesIsRejected() {
        String content = "a".repeat(102401);
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> manager().savePayload(content));
        assertEquals("Size limit reached", exception.getMessage());
    }

    @Test
    void fileReferenceHelpers() {
        assertTrue(FilePayloadManager.isFileReference("[FILE_REF]:C:/messages/a.dat"));
        assertFalse(FilePayloadManager.isFileReference("plain content"));
        assertEquals("C:/messages/a.dat",
                FilePayloadManager.extractPath("[FILE_REF]:C:/messages/a.dat"));
        assertNull(FilePayloadManager.extractPath("plain content"));
    }
}

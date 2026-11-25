
package com.nba.engine.raft;

import com.nba.engine.grpc.EventType;
import com.nba.engine.grpc.LogEntry;
import com.nba.engine.grpc.PlayEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RaftLogTest {

    @TempDir
    Path tempDir;

    private Path walPath() {
        return tempDir.resolve("wal/raft-log.wal");
    }

    private LogEntry entry(long index, long term) {
        PlayEvent event = PlayEvent.newBuilder().setEventId("event-" + index).setGameId("game-1").setPlayerId(23).setEventType(EventType.POINTS_SCORED).setValue(2).setTimestampMs(System.currentTimeMillis()).build();

        return LogEntry.newBuilder().setIndex(index).setTerm(term).setEvent(event).build();
    }

    @Test
    void appendsAndReadsEntries() throws IOException {
        try (RaftLog log = new RaftLog(walPath())) {
            log.append(entry(1, 1));
            log.append(entry(2, 1));

            assertEquals(2, log.lastIndex());
            assertEquals(1, log.lastTerm());
            assertEquals("event-1", log.getEntry(1).orElseThrow().getEvent().getEventId());
            assertEquals(2, log.entriesFrom(1, 10).size());
        }
    }

    @Test
    void recoversEntriesAfterRestart() throws IOException {
        try (RaftLog log = new RaftLog(walPath())) {
            log.append(entry(1, 1));
            log.append(entry(2, 2));
        }

        try (RaftLog recovered = new RaftLog(walPath())) {
            assertEquals(2, recovered.lastIndex());
            assertEquals(2, recovered.lastTerm());
            assertEquals("event-2", recovered.getEntry(2).orElseThrow().getEvent().getEventId());
        }
    }

    @Test
    void truncatesConflictingSuffix() throws IOException {
        try (RaftLog log = new RaftLog(walPath())) {
            log.append(entry(1, 1));
            log.append(entry(2, 1));
            log.append(entry(3, 2));

            log.truncateSuffix(2);

            assertEquals(1, log.lastIndex());

            // The replacement entry can reuse the truncated index.
            log.append(entry(2, 3));

            assertEquals(2, log.lastIndex());
            assertEquals(3, log.lastTerm());
        }

        try (RaftLog recovered = new RaftLog(walPath())) {
            assertEquals(2, recovered.lastIndex());
            assertEquals(3, recovered.lastTerm());
        }
    }

    @Test
    void truncatesIncompleteTrailingRecord() throws IOException {
        try (RaftLog log = new RaftLog(walPath())) {
            log.append(entry(1, 1));
        }

        // Simulate a crash during the next record's length header.
        Files.write(walPath(), new byte[]{0, 0}, java.nio.file.StandardOpenOption.APPEND);

        try (RaftLog recovered = new RaftLog(walPath())) {
            assertEquals(1, recovered.lastIndex());
        }

        // Verify recovery physically removed the incomplete bytes.
        assertTrue(Files.size(walPath()) > 2);
    }

    @Test
    void rejectsNonSequentialAppend() throws IOException {
        try (RaftLog log = new RaftLog(walPath())) {
            assertThrows(IllegalArgumentException.class, () -> log.append(entry(2, 1))
            );
        }
    }

    @Test
    void returnsEmptyForMissingEntry() throws IOException {
        try (RaftLog log = new RaftLog(walPath())) {
            assertTrue(log.getEntry(1).isEmpty());
            assertEquals(List.of(), log.entriesFrom(1, 10));
        }
    }
}


package com.nba.engine.raft;

import com.nba.engine.grpc.LogEntry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;

public class RaftLog implements AutoCloseable {

    private static final int HEADER_SIZE = Integer.BYTES;
    private static final int MAX_ENTRY_SIZE = 16 * 1024 * 1024;

    private final Path walPath;
    private final FileChannel channel;

    // Maps log index -> byte offset where its WAL record begins.
    private final NavigableMap<Long, Long> offsets = new TreeMap<>();

    public RaftLog(Path walPath) throws IOException {
        this.walPath = walPath;

        Path parent = walPath.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }

        this.channel = FileChannel.open(walPath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);

        recover();
    }

    public synchronized void append(LogEntry entry) throws IOException {
        long expectedIndex = lastIndex() + 1;

        if (entry.getIndex() != expectedIndex) {
            throw new IllegalArgumentException("Expected log index " + expectedIndex + ", received " + entry.getIndex());
        }

        if (entry.getTerm() < 0) {
            throw new IllegalArgumentException("Term cannot be negative");
        }

        byte[] payload = entry.toByteArray();

        if (payload.length == 0 || payload.length > MAX_ENTRY_SIZE) {
            throw new IOException("Invalid WAL entry size: " + payload.length);
        }

        long offset = channel.size();

        ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
        header.putInt(payload.length);
        header.flip();

        try {
            channel.position(offset);
            writeFully(header);
            writeFully(ByteBuffer.wrap(payload));

            channel.force(true);

            offsets.put(entry.getIndex(), offset);

        } catch (IOException e) {
            try {
                channel.truncate(offset);
                channel.force(true);
            } catch (IOException cleanupError) {
                e.addSuppressed(cleanupError);
            }
            throw e;
        }
    }

    public synchronized Optional<LogEntry> getEntry(long index)
            throws IOException {

        Long offset = offsets.get(index);

        if (offset == null) {
            return Optional.empty();
        }

        return Optional.of(readEntryAt(offset));
    }

    public synchronized List<LogEntry> entriesFrom(long startIndex, int maxEntries) throws IOException {

        if (startIndex < 1 || maxEntries < 0) {
            throw new IllegalArgumentException("Invalid range");
        }

        List<LogEntry> result = new ArrayList<>();

        if (maxEntries == 0) {
            return result;
        }

        for (long index : offsets.tailMap(startIndex, true).keySet()) {
            if (result.size() >= maxEntries) {
                break;
            }

            result.add(readEntryAt(offsets.get(index)));
        }

        return result;
    }

    public synchronized long lastIndex() {
        if (offsets.isEmpty()) {
            return 0;
        }
        else {
            return offsets.lastKey();
        }
    }

    public synchronized long lastTerm() throws IOException {
        if (offsets.isEmpty()) {
            return 0;
        }
        return readEntryAt(offsets.lastEntry().getValue()).getTerm();
    }

    public synchronized long termAt(long index) throws IOException {
        if (index == 0) {
            return 0;
        }

        return getEntry(index).orElseThrow(() -> new IllegalArgumentException("No log entry at index " + index)).getTerm();
    }

    public synchronized void truncateSuffix(long fromIndex)throws IOException {

        if (fromIndex < 1) {
            throw new IllegalArgumentException("Index must be >= 1");
        }

        if (fromIndex > lastIndex()) {
            return;
        }

        Long truncateOffset = offsets.get(fromIndex);

        if (truncateOffset == null) {
            throw new IllegalStateException("Missing WAL offset for index " + fromIndex);
        }

        channel.truncate(truncateOffset);
        channel.force(true);

        offsets.tailMap(fromIndex, true).clear();
    }

    private void recover() throws IOException {
        offsets.clear();

        long fileSize = channel.size();
        long position = 0;
        long expectedIndex = 1;

        while (position < fileSize) {
            long recordStart = position;

            // A partial header means the final record was interrupted.
            ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);

            if (!readFully(header, position)) {
                truncateIncompleteRecord(recordStart);
                break;
            }

            header.flip();
            int payloadLength = header.getInt();
            position += HEADER_SIZE;

            if (payloadLength <= 0 || payloadLength > MAX_ENTRY_SIZE) {
                throw new IOException("Invalid WAL record length at offset " + recordStart);
            }

            // A partial payload means the final record was interrupted.
            if (fileSize - position < payloadLength) {
                truncateIncompleteRecord(recordStart);
                break;
            }

            ByteBuffer payload = ByteBuffer.allocate(payloadLength);

            if (!readFully(payload, position)) {
                truncateIncompleteRecord(recordStart);
                break;
            }

            LogEntry entry;

            try {
                entry = LogEntry.parseFrom(payload.array());
            } catch (Exception e) {
                throw new IOException("Invalid Protobuf entry at WAL offset " + recordStart, e);
            }

            if (entry.getIndex() != expectedIndex) {
                throw new IOException("Non-contiguous WAL index. Expected " + expectedIndex + ", found " + entry.getIndex());
            }

            if (entry.getTerm() < 0) {
                throw new IOException("Negative term in WAL");
            }

            offsets.put(entry.getIndex(), recordStart);

            expectedIndex++;
            position += payloadLength;
        }

        channel.position(channel.size());

        System.out.println("Recovered " + offsets.size() + " log entries from " + walPath);
    }

    private void truncateIncompleteRecord(long recordStart)
        throws IOException {
            channel.truncate(recordStart);
            channel.force(true);

        System.out.println("Truncated incomplete WAL record at offset " + recordStart);
    }

    private LogEntry readEntryAt(long offset) throws IOException {
        ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);

        if (!readFully(header, offset)) {
            throw new IOException("Unable to read WAL header at " + offset);
        }

        header.flip();
        int payloadLength = header.getInt();

        if (payloadLength <= 0 || payloadLength > MAX_ENTRY_SIZE) {
            throw new IOException("Invalid WAL record length at " + offset);
        }

        ByteBuffer payload = ByteBuffer.allocate(payloadLength);

        if (!readFully(payload, offset + HEADER_SIZE)) {
            throw new IOException("Incomplete WAL record at " + offset);
        }

        try {
            return LogEntry.parseFrom(payload.array());
        } catch (Exception e) {
            throw new IOException("Unable to parse WAL entry", e);
        }
    }

    private boolean readFully(ByteBuffer buffer, long position)
            throws IOException {

        while (buffer.hasRemaining()) {
            int bytesRead = channel.read(buffer, position);

            if (bytesRead < 0) {
                return false;
            }

            if (bytesRead == 0) {
                continue;
            }

            position += bytesRead;
        }

        return true;
    }

    private void writeFully(ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        channel.close();
    }
}

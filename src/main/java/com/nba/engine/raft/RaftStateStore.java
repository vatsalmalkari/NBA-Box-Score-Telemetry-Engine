package com.nba.engine.raft;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Properties;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.*;

public class RaftStateStore {

    private final Path stateFile;

    public RaftStateStore(String filePath) throws IOException {
        this.stateFile = Paths.get(filePath);

        Path parent = stateFile.getParent();

        if (parent != null) {
            Files.createDirectories(parent);
        }
    }

    public synchronized long loadTerm() throws IOException {
        Properties properties = loadProperties();

        return Long.parseLong(
                properties.getProperty("currentTerm", "0")
        );
    }

    public synchronized String loadVotedFor() throws IOException {
        String value = loadProperties().getProperty("votedFor");
        return value == null || value.isBlank() ? null : value;
    }

    public synchronized void save(long term, String votedFor)
            throws IOException {

        Properties properties = new Properties();
        properties.setProperty("currentTerm", Long.toString(term));

        if (votedFor != null) {
            properties.setProperty("votedFor", votedFor);
        }

        Path tempFile = stateFile.resolveSibling(
                stateFile.getFileName() + ".tmp"
        );

        StringBuilder content = new StringBuilder();
        content.append("currentTerm=").append(term).append('\n');

        if (votedFor != null) {
            content.append("votedFor=").append(votedFor).append('\n');
        }

        byte[] bytes = content.toString()
                .getBytes(StandardCharsets.UTF_8);

        try (FileChannel channel = FileChannel.open(
                tempFile, CREATE, WRITE, TRUNCATE_EXISTING)) {

            ByteBuffer buffer = ByteBuffer.wrap(bytes);

            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }

            channel.force(true);
        }

        try {
            Files.move(tempFile, stateFile, ATOMIC_MOVE, REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(tempFile, stateFile, REPLACE_EXISTING);
        }
    }

    private Properties loadProperties() throws IOException {
        Properties properties = new Properties();

        if (Files.exists(stateFile)) {
            try (var input = Files.newInputStream(stateFile)) {
                properties.load(input);
            }
        }

        return properties;
    }
}
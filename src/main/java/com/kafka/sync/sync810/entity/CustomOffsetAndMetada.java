package com.kafka.sync.sync810.entity;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;

import java.util.Optional;

/**
 * 继承OffsetAndMetadata，增加timestamp，用于判断消息的先后顺序
 */
public class CustomOffsetAndMetada extends OffsetAndMetadata {
    private long timestamp;

    public CustomOffsetAndMetada(long offset, Optional<Integer> leaderEpoch, String metadata) {
        super(offset, leaderEpoch, metadata);
    }

    public CustomOffsetAndMetada(long offset, String metadata) {
        super(offset, metadata);
    }

    public CustomOffsetAndMetada(long offset) {
        super(offset);
    }

    public CustomOffsetAndMetada(long offset, String metadata, long timestamp) {
        super(offset, metadata);
        this.timestamp = timestamp;
    }

    public CustomOffsetAndMetada(long offset, long timestamp) {
        super(offset);
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}

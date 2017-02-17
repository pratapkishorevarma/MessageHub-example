package com.ibm.messageHub.beans;

public class TopicConfig {

    private long retentionMs;

    public TopicConfig() {
        super();
    }

    public TopicConfig(long retentionMs) {
        super();
        this.retentionMs = retentionMs;
    }

    public void setRetentionMs(long retentionMs) {

        this.retentionMs = retentionMs;
    }

    public long getRetentionMs() {

        return retentionMs;
    }
}

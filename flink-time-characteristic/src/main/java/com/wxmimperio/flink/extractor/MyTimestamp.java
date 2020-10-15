package com.wxmimperio.flink.extractor;

import java.io.Serializable;

public class MyTimestamp implements Serializable {
    private Long currentMinTimestamp;
    private long lastUpdateTime;

    public MyTimestamp() {
    }

    public MyTimestamp(Long currentMinTimestamp, long lastUpdateTime) {
        this.currentMinTimestamp = currentMinTimestamp;
        this.lastUpdateTime = lastUpdateTime;
    }

    public Long getCurrentMinTimestamp() {
        return currentMinTimestamp;
    }

    public void setCurrentMinTimestamp(Long currentMinTimestamp) {
        this.currentMinTimestamp = currentMinTimestamp;
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }
}

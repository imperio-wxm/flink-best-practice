package com.wxmimperio.flink;

import org.roaringbitmap.longlong.Roaring64NavigableMap;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className PreciseAccumulator.java
 * @description This is the description of PreciseAccumulator.java
 * @createTime 2021-01-06 19:14:00
 */
public class PreciseAccumulator {
    private final Roaring64NavigableMap bitmap;

    public PreciseAccumulator() {
        bitmap = new Roaring64NavigableMap();
    }

    public void add(long id) {
        bitmap.addLong(id);
    }

    public long getCardinality() {
        return bitmap.getLongCardinality();
    }
}

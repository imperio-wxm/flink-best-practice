package com.wxmimperio.flink;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public abstract class MyWatermark<T> implements AssignerWithPeriodicWatermarks<T> {

    private static final long serialVersionUID = 1L;

    /**
     * The current maximum timestamp seen so far.
     */
    private long currentMaxTimestamp;

    /**
     * The timestamp of the last emitted watermark.
     */
    private long lastEmittedWatermark = Long.MIN_VALUE;

    /**
     * The (fixed) interval between the maximum seen timestamp seen in the records
     * and that of the watermark to be emitted.
     */
    private final long maxOutOfOrderness;

    private final Cache<Long, Long> manualCache;

    public MyWatermark(Time maxOutOfOrderness, Time maxForceRefresh) {
        if (maxOutOfOrderness.toMilliseconds() < 0L) {
            throw new RuntimeException("Tried to set the maximum allowed lateness to " + maxOutOfOrderness + ". This parameter cannot be negative.");
        } else {
            this.maxOutOfOrderness = maxOutOfOrderness.toMilliseconds();
            this.currentMaxTimestamp = Long.MIN_VALUE + this.maxOutOfOrderness;
            this.manualCache = Caffeine.newBuilder()
                    .expireAfterWrite(maxForceRefresh.toMilliseconds(), TimeUnit.MILLISECONDS)
                    .maximumSize(120)
                    .build();
        }
    }

    public long getMaxOutOfOrdernessInMillis() {
        return this.maxOutOfOrderness;
    }

    public abstract long extractTimestamp(T var1);

    @Override
    public final Watermark getCurrentWatermark() {
        long minKey = Long.MAX_VALUE;
        for (Map.Entry<Long, Long> entry : manualCache.asMap().entrySet()) {
            long key = entry.getKey();
            if (key < minKey) {
                minKey = key;
            }
        }
        long potentialWM = getCache(minKey);
        if (potentialWM >= this.lastEmittedWatermark) {
            this.lastEmittedWatermark = potentialWM;
        }
        System.out.println(manualCache.asMap().size());
        return new Watermark(this.lastEmittedWatermark);
    }

    @Override
    public final long extractTimestamp(T element, long previousElementTimestamp) {
        long timestamp = this.extractTimestamp(element);
        long delayTimestamp = getDelayTimestamp(timestamp);
        long timeKey = delayTimestamp / 10000;
        if (manualCache.asMap().containsKey(timeKey)) {
            if (delayTimestamp < manualCache.asMap().get(timeKey)) {
                manualCache.put(timeKey, delayTimestamp);
            }
        } else {
            manualCache.put(timeKey, delayTimestamp);
        }
        if (delayTimestamp < lastEmittedWatermark) {
            System.out.println("!!!!!!!!");
        }
        return timestamp;
    }

    private long getDelayTimestamp(long timestamp) {
        return timestamp - maxOutOfOrderness;
    }

    private long getCache(long key) {
        return manualCache.get(key, new Function<Long, Long>() {
            @Override
            public Long apply(Long key) {
                return Long.MIN_VALUE;
            }
        });
    }

    public static void main(String[] args) {
        long time = System.currentTimeMillis();
        System.out.println(time / 10000);
        System.out.println(time / (5000));
    }
}

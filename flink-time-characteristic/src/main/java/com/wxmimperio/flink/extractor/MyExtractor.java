package com.wxmimperio.flink.extractor;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

public abstract class MyExtractor<T> implements AssignerWithPeriodicWatermarks<T> {

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

    private final MyTimestamp myTimestamp;
    private final long defaultTimestamp;

    public MyExtractor(Time maxOutOfOrderness) {
        if (maxOutOfOrderness.toMilliseconds() < 0) {
            throw new RuntimeException("Tried to set the maximum allowed " +
                    "lateness to " + maxOutOfOrderness + ". This parameter cannot be negative.");
        }
        this.maxOutOfOrderness = maxOutOfOrderness.toMilliseconds();
        this.currentMaxTimestamp = Long.MIN_VALUE + this.maxOutOfOrderness;
        this.myTimestamp = new MyTimestamp(Long.MAX_VALUE, System.currentTimeMillis());
        this.defaultTimestamp = System.currentTimeMillis();
    }

    public long getMaxOutOfOrdernessInMillis() {
        return maxOutOfOrderness;
    }

    /**
     * Extracts the timestamp from the given element.
     *
     * @param element The element that the timestamp is extracted from.
     * @return The new timestamp.
     */
    public abstract long extractTimestamp(T element);

    @Override
    public final Watermark getCurrentWatermark() {
        System.out.println(System.currentTimeMillis() + " 调用！！！");
        if (((System.currentTimeMillis() - myTimestamp.getLastUpdateTime()) / 1000) > 30) {
            myTimestamp.setCurrentMinTimestamp(currentMaxTimestamp - maxOutOfOrderness);
            myTimestamp.setLastUpdateTime(System.currentTimeMillis());
            System.out.println("Mandatory update watermark = {}, lastUpdateTime = {}" + myTimestamp.getCurrentMinTimestamp() + "," + myTimestamp.getLastUpdateTime());
        }
        // this guarantees that the watermark never goes backwards.
        if (myTimestamp.getCurrentMinTimestamp() >= lastEmittedWatermark) {
            lastEmittedWatermark = myTimestamp.getCurrentMinTimestamp();
        }
        System.out.println("lastEmittedWatermark = " + lastEmittedWatermark);
        return new Watermark(lastEmittedWatermark);
    }

    @Override
    public final long extractTimestamp(T element, long previousElementTimestamp) {
        long timestamp = extractTimestamp(element);
        if (timestamp > currentMaxTimestamp) {
            currentMaxTimestamp = timestamp;
        }
        if (null == myTimestamp.getCurrentMinTimestamp() ||
                timestamp < myTimestamp.getCurrentMinTimestamp()) {
            // 更新最小值
            System.out.println("存储最小");
            myTimestamp.setCurrentMinTimestamp(timestamp - maxOutOfOrderness);
        }
        return timestamp;
    }
}

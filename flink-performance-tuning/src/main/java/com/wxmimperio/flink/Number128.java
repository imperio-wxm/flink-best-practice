package com.wxmimperio.flink;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className Number128.java
 * @description This is the description of Number128.java
 * @createTime 2021-01-06 20:30:00
 */
public class Number128 {
    private long lowValue;
    private long highValue;

    /**
     * 构造
     *
     * @param lowValue  低位
     * @param highValue 高位
     */
    public Number128(long lowValue, long highValue) {
        this.lowValue = lowValue;
        this.highValue = highValue;
    }

    public long getLowValue() {
        return lowValue;
    }

    public long getHighValue() {
        return highValue;
    }

    public void setLowValue(long lowValue) {
        this.lowValue = lowValue;
    }

    public void setHighValue(long hiValue) {
        this.highValue = hiValue;
    }

    public long[] getLongArray() {
        return new long[]{lowValue, highValue};
    }

}

package com.wxmimperio.flink;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.functions.AggregateFunction;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className PreciseDistinct.java
 * @description This is the description of PreciseDistinct.java
 * @createTime 2021-01-06 19:15:00
 */
public class PreciseDistinct extends AggregateFunction<Long, PreciseAccumulator> {

    @Override
    public PreciseAccumulator createAccumulator() {
        return new PreciseAccumulator();
    }

    public void accumulate(PreciseAccumulator accumulator, Integer id) {
        accumulator.add(id);
    }

    public void accumulate(PreciseAccumulator accumulator, Long id) {
        accumulator.add(id);
    }

    public void accumulate(PreciseAccumulator accumulator, String id) {
        accumulator.add(CityHash.hash64(id.getBytes()));
    }

    public void accumulate(PreciseAccumulator accumulator, List<String> list) {
        accumulator.add(CityHash.hash64(StringUtils.join(list.toArray()).getBytes()));
    }

    public void accumulate(PreciseAccumulator accumulator, String[] list) {
        accumulator.add(CityHash.hash64(StringUtils.join(list).getBytes()));
    }

    @Override
    public Long getValue(PreciseAccumulator acc) {
        return acc.getCardinality();
    }
}

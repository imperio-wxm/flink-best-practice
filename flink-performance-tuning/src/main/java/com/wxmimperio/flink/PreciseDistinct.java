package com.wxmimperio.flink;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.FunctionRequirement;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Set;

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
        String key = StringUtils.join(list.toArray());
        accumulator.add(CityHash.hash64(key.getBytes()));
    }

    public void accumulate(PreciseAccumulator accumulator, String[] list) {
        String key = StringUtils.join(list);
        accumulator.add(CityHash.hash64(key.getBytes()));
    }

    @Override
    public Long getValue(PreciseAccumulator acc) {
        return acc.getCardinality();
    }
}

package com.wxmimperio.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class BaseOperators {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Tuple2<Long, Long>> dataStream = env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L));
        dataStream.print("src = ");

        // map
        DataStream<Tuple2<Long, Long>> map = dataStream.map(new MapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>() {
            @Override
            public Tuple2<Long, Long> map(Tuple2<Long, Long> value) throws Exception {
                return Tuple2.of(value.f0 * 2, value.f1 * 2);
            }
        });
        map.print("map = ");

        // FlatMap
        DataStream<Long> flatMap = map.flatMap(new FlatMapFunction<Tuple2<Long, Long>, Long>() {
            @Override
            public void flatMap(Tuple2<Long, Long> value, Collector<Long> out) throws Exception {
                out.collect(value.f0 + value.f1);
            }
        });
        flatMap.print("flatMap = ");

        // Filter
        DataStream<Long> filter = flatMap.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value > 8;
            }
        });
        filter.print("filter = ");

        // KeyBy
        DataStream<Tuple2<Long, Long>> keyByStream = env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L));
        // Key by field "someKey"
        // Key by the first element of a Tuple
        KeyedStream<Tuple2<Long, Long>, Tuple> keyStream = keyByStream.keyBy(0);
        keyStream.print("keyBy = ");

        // Reduce
        DataStream<Tuple2<Long, Long>> reduce = keyStream.reduce(new ReduceFunction<Tuple2<Long, Long>>() {
            @Override
            public Tuple2<Long, Long> reduce(Tuple2<Long, Long> value1, Tuple2<Long, Long> value2) throws Exception {
                return Tuple2.of(value1.f0 + value2.f0, value1.f1 + value2.f1);
            }
        });
        reduce.print("reduce = ");


        env.execute("KeyedState");
    }
}

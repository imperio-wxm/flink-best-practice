package com.wxmimperio.flink;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class KeyedState {

    /**
     * output:
     * (1,4)
     * (1,5)
     * (1,6)
     * (2,11)
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // this can be used in a streaming program like this (assuming we have a StreamExecutionEnvironment env)
        env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(1L, 4L), Tuple2.of(1L, 2L), Tuple2.of(1L, 10L),
                Tuple2.of(2L, 10L), Tuple2.of(2L, 12L))
                .keyBy(0)
                .flatMap(new CountWindowAverage())
                .print();

        env.execute("KeyedState");
    }

    public static class CountWindowAverage extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

        /**
         * The ValueState handle. The first field is the count, the second field a running sum.
         */
        private transient ValueState<Tuple2<Long, Long>> sum;

        @Override
        public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Long>> out) throws Exception {
            // access the state value
            Tuple2<Long, Long> currentSum = sum.value();

            // update the count
            currentSum.f0 += 1;

            // add the second field of the input value
            currentSum.f1 += value.f1;

            // update the state
            sum.update(currentSum);

            if (currentSum.f0 >= 2) {
                System.out.println("==============");
                out.collect(new Tuple2<>(value.f0, currentSum.f1 / currentSum.f0));
                sum.clear();
            }
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Tuple2<Long, Long>> descriptor = new ValueStateDescriptor<Tuple2<Long, Long>>(
                    "average", // state name
                    TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
                    }),  // type information
                    Tuple2.of(0L, 0L)  // default value of the state, if nothing was set
            );
            // enable state ttl
            // Only TTLs in reference to processing time are currently supported.
            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(Time.seconds(10))
                    // .disableCleanupInBackground()
                    // OnCreateAndWrite only on creation and write access
                    // OnReadAndWrite also on read access
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    // NeverReturnExpired expired value is never returned
                    // ReturnExpiredIfNotCleanedUp returned if still available, allows to return the expired state before its cleanup
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();
            descriptor.enableTimeToLive(ttlConfig);
            sum = getRuntimeContext().getState(descriptor);
        }
    }
}

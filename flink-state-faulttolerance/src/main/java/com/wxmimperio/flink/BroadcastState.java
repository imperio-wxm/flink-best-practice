package com.wxmimperio.flink;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

public class BroadcastState {

    /**
     * Broadcast state was introduced to support use cases where some data coming from one stream is required to be broadcasted to all downstream tasks,
     * where it is stored locally and is used to process all incoming elements on the other stream
     * <p>
     * The connect should be called on the non-broadcasted stream, with the BroadcastStream as an argument
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // non-broadcasted stream
        DataStreamSource<String> originStream = env.socketTextStream("10.1.8.210", 9999);

        MapStateDescriptor<String, String> descriptor = new MapStateDescriptor<>(
                "dynamicConfig",
                TypeInformation.of(new TypeHint<String>() {
                }),
                TypeInformation.of(new TypeHint<String>() {
                })
        );

        // broadcasted stream
        BroadcastStream<Tuple2<String, String>> configStream = env.addSource(new DynamicConfigSource()).broadcast(descriptor);
        // The connect should be called on the non-broadcasted stream, with the BroadcastStream as an argument.
        BroadcastConnectedStream<String, Tuple2<String, String>> connectStream = originStream.connect(configStream);
        connectStream.process(new BroadcastProcessFunction<String, Tuple2<String, String>, Void>() {
            // Used for the non-broadcasted one.
            @Override
            public void processElement(String value, ReadOnlyContext ctx, Collector<Void> out) throws Exception {
                ReadOnlyBroadcastState<String, String> config = ctx.getBroadcastState(descriptor);
                ctx.timestamp();
                ctx.currentWatermark();
                ctx.currentProcessingTime();
                // 这里的名字必须是state的名字
                String configValue = config.get("demoConfigKey");
                //do some process base on the config
                System.out.println("process value = " + value + ", config = " + configValue);
            }

            // Processing incoming elements in the broadcasted stream.
            @Override
            public void processBroadcastElement(Tuple2<String, String> value, Context ctx, Collector<Void> out) throws Exception {
                System.out.println("receive config item: " + value);
                //update state
                ctx.getBroadcastState(descriptor).put(value.getField(0), value.getField(1));
            }
        });

        env.execute("testBroadcastState");
    }

    /**
     * 定义一个source，每隔10s更新一次
     */
    public static class DynamicConfigSource implements SourceFunction<Tuple2<String, String>> {
        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
            long idx = 1;
            while (isRunning) {
                ctx.collect(Tuple2.of("demoConfigKey", "value" + idx));
                idx++;
                TimeUnit.SECONDS.sleep(5);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}

package com.wxmimperio.flink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.wxmimperio.flink.extractor.MyExtractor;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.util.Collector;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class FlinkKafkaWaterMark {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setBoolean(ConfigConstants.LOCAL_NUMBER_JOB_MANAGER, true);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.enableCheckpointing(20000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.getConfig().setAutoWatermarkInterval(30 * 1000);

        // 2020-08-24 15:13:50
        // 2020-08-24 15:13:20
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("group.id", "flink-test");

        FlinkKafkaConsumerBase<String> myConsumer = new FlinkKafkaConsumer<>(
                /*"wxm_wk_test01",*/
                "wxm_flink_test02",
                new SimpleStringSchema(),
                properties
        );

        DataStream<String> dataStreamSource = env.addSource(myConsumer)
                .setParallelism(8)
                .assignTimestampsAndWatermarks(new MyExtractor<String>(Time.seconds(300)) {
                    @Override
                    public long extractTimestamp(String element) {
                        JSONObject jsonObject = JSON.parseObject(element);
                        return Long.parseLong(jsonObject.getString("event_time"));
                    }
                })
                /*.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(60)) {
                    @Override
                    public long extractTimestamp(String s) {
                        JSONObject jsonObject = JSON.parseObject(s);
                        return Long.parseLong(jsonObject.getString("event_time"));
                    }
                })*/;

        DataStream<String> op1 = dataStreamSource.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                jsonObject.put("add_col1", "process1");
                out.collect(jsonObject.toJSONString());
            }
        }).setParallelism(8);

        DataStream<String> op2 = op1.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                jsonObject.put("add_col2", "process2");
                out.collect(jsonObject.toJSONString());
            }
        }).name("process2").setParallelism(8);

        DataStream<String> op3 = op2.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                jsonObject.put("add_col3", "process3");
                out.collect(jsonObject.toJSONString());
            }
        }).name("process3").setParallelism(7);

        op3.print().setParallelism(1).name("sink");

        env.execute("test");

    }

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

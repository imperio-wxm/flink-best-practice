package com.wxmimperio.flink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

public class FlinkKafkaWaterMark {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setBoolean(ConfigConstants.LOCAL_NUMBER_JOB_MANAGER, true);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.enableCheckpointing(20000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.getConfig().setAutoWatermarkInterval(1000);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("group.id", "flink-test");

        FlinkKafkaConsumerBase<String> myConsumer = new FlinkKafkaConsumer<>(
                /*"wxm_wk_test01",*/
                "wxm_wk_test",
                new SimpleStringSchema(),
                properties
        ).assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<String>() {
            @Nullable
            @Override
            public Watermark checkAndGetNextWatermark(String s, long l) {
                if(l == Long.MIN_VALUE) {
                    return null;
                }
                return JSON.parseObject(s).containsKey("event_time") ? new Watermark(l) : null;
            }

            @Override
            public long extractTimestamp(String s, long l) {
                return Long.parseLong(JSON.parseObject(s).getString("event_time"));
            }
        });

        DataStream<String> dataStreamSource = env.addSource(myConsumer)
                .setParallelism(3).rescale();
        //.rescale()
                /*.assignTimestampsAndWatermarks(new MyExtractor<String>(Time.seconds(5)) {
                    @Override
                    public long extractTimestamp(String s) {
                        JSONObject jsonObject = JSON.parseObject(s);
                        return Long.parseLong(jsonObject.getString("event_time"));
                    }
                });*/

        DataStream<String> op1 = dataStreamSource.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String s, Context context, Collector<String> collector) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                jsonObject.put("add_col1", "process1");
                collector.collect(jsonObject.toJSONString());
            }
        }).name("process1").setParallelism(2);

        DataStream<String> op2 = op1.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String s, Context context, Collector<String> collector) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                jsonObject.put("add_col2", "process2");
                collector.collect(jsonObject.toJSONString());
            }
        }).name("process2").setParallelism(3);


        op2.print().setParallelism(1).name("sink");

        env.execute("test");

    }
}

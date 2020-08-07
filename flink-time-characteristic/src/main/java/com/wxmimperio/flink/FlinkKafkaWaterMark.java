package com.wxmimperio.flink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.util.Collector;

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
                "wxm_wk_test01",
                new SimpleStringSchema(),
                properties
        ).setStartFromTimestamp(1593704292000L)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(5)) {
                    @Override
                    public long extractTimestamp(String s) {
                        JSONObject jsonObject = JSON.parseObject(s);
                        return Long.parseLong(jsonObject.getString("event_time"));
                    }
                });

        DataStream<String> dataStreamSource = env.addSource(myConsumer)
                .setParallelism(3);

        DataStream<String> op1 = dataStreamSource.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String s, Context context, Collector<String> collector) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                jsonObject.put("add_col1", "process1");
                collector.collect(jsonObject.toJSONString());
            }
        }).name("process1").setParallelism(3);

        DataStream<String> op2 = op1.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String s, Context context, Collector<String> collector) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                jsonObject.put("add_col2", "process2");
                collector.collect(jsonObject.toJSONString());
            }
        }).name("process2").setParallelism(2);


        op2.print().setParallelism(1).name("sink");

        env.execute("test");

    }
}

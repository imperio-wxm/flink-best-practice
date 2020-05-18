package com.wxmimperio.flink.connecter.kafka;

import com.alibaba.fastjson.JSONObject;
import com.wxmimperio.flink.serialization.AvroDeserializationToJson;
import org.apache.avro.Schema;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

public class KafkaSource {

    public static void main(String[] args) throws Exception {
        /*Configuration conf = new Configuration();
        conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);*/
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.enableCheckpointing(5000);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.174.20.33:9092");
        properties.setProperty("group.id", "flink-test");
        String path = "D:\\d_backup\\github\\flink-best-practice\\flink-simple-demo\\src\\main\\resources\\test.avsc";
        Schema schema = new Schema.Parser().parse(new FileInputStream(new File(path)));

        FlinkKafkaConsumer<JSONObject> myConsumer = new FlinkKafkaConsumer<>(
                "test_flink1",
                new AvroDeserializationToJson(AvroDeserializationSchema.forGeneric(schema)),
                properties
        );

        DataStream<JSONObject> stream = env.addSource(myConsumer);

        stream.filter(data -> {
            return data.containsKey("group_id") && data.getIntValue("group_id") == 1;
        }).keyBy((KeySelector<JSONObject, Object>) jsonObject -> {
            String eventTime = jsonObject.getString("event_time").substring(0, 10);
            String characterId = jsonObject.getString("character_id");
            return eventTime + "_" + characterId;
        }).reduce((ReduceFunction<JSONObject>) (oldData, newData) -> {
            int oldValue = oldData.getIntValue("experience");
            int newValue = newData.getIntValue("experience");
            newData.put("experience", oldValue + newValue);
            return newData;
        }).filter((FilterFunction<JSONObject>) jsonObject -> {
            if (jsonObject.getIntValue("experience") > 10) {
                return true;
            } else {
                System.out.println(String.format("experience = %s; character_id = %s,不满足条件，不输出",
                        jsonObject.getString("experience"),
                        jsonObject.getString("character_id")
                ));
                return false;
            }
        }).map(data -> {
            return String.format("输出 experience = %s, character_id = %s", data.getString("experience"), data.getString("character_id"));
        }).print().setParallelism(1);

        env.execute("Kafka data");
    }
}

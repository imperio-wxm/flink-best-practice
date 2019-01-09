package com.wxmimperio.flink.connecter.kafka;

import avro.shaded.com.google.common.collect.Maps;
import com.alibaba.fastjson.JSONObject;
import com.wxmimperio.flink.serialization.AvroDeserializationToJson;
import org.apache.avro.Schema;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.io.File;
import java.io.FileInputStream;
import java.util.Map;
import java.util.Properties;

public class KafkaSource {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        //final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.enableCheckpointing(3000 * 10);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.1.110:9092");
        //properties.setProperty("zookeeper.connect", "192.168.1.20:2181");
        properties.setProperty("group.id", "flink-test");
        //String schenaStr = "{\"type\":\"record\",\"name\":\"rtc_warning_gmys\",\"namespace\":\"com.sdo.dw.rtc\",\"doc\":\"rtc_warning_gmys\",\"fields\":[{\"name\":\"table_name\",\"type\":[\"string\",\"null\"],\"doc\":\"告警源表\",\"size\":\"0\"},{\"name\":\"character_id\",\"type\":[\"string\",\"null\"],\"doc\":\"角色ID\",\"size\":\"0\"},{\"name\":\"area_id\",\"type\":[\"int\",\"null\"],\"doc\":\"区ID\",\"size\":\"0\"},{\"name\":\"group_id\",\"type\":[\"int\",\"null\"],\"doc\":\"服ID\",\"size\":\"0\"},{\"name\":\"platform\",\"type\":[\"int\",\"null\"],\"doc\":\"平台ID\",\"size\":\"0\"},{\"name\":\"character_level\",\"type\":[\"int\",\"null\"],\"doc\":\"角色等级\",\"size\":\"0\"},{\"name\":\"warn_column\",\"type\":[\"string\",\"null\"],\"doc\":\"设置告警的字段\",\"size\":\"0\"},{\"name\":\"current_value\",\"type\":[\"string\",\"null\"],\"doc\":\"当前值\",\"size\":\"0\"},{\"name\":\"total_value\",\"type\":[\"string\",\"null\"],\"doc\":\"累计总值\",\"size\":\"0\"},{\"name\":\"event_time\",\"type\":[\"string\",\"null\"],\"doc\":\"数据源时间\",\"size\":\"0\"},{\"name\":\"warn_type\",\"type\":[\"string\",\"null\"],\"doc\":\"异常类型\",\"size\":\"0\"}],\"last_update_time\":\"Tue Dec 11 15:36:03 CST 2018\"}";
        String path = "E:\\coding\\github\\flink-best-practice\\flink-simple-demo\\src\\main\\resources\\test.avro";
        Schema schema = new Schema.Parser().parse(new FileInputStream(new File(path)));

        FlinkKafkaConsumer011<JSONObject> myConsumer = new FlinkKafkaConsumer011<>(
                "stream-test1",
                //ConfluentRegistryAvroDeserializationSchema.forGeneric(schema),
                new AvroDeserializationToJson(AvroDeserializationSchema.forGeneric(schema)),
                properties
        );

        DataStream<JSONObject> stream = env.addSource(myConsumer);

        stream.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                return jsonObject.containsKey("group_id") && jsonObject.getIntValue("group_id") == 1;
            }
        }).keyBy(new KeySelector<JSONObject, Object>() {
            @Override
            public Object getKey(JSONObject jsonObject) throws Exception {
                String eventTime = jsonObject.getString("event_time").substring(0, 10);
                String characterId = jsonObject.getString("character_id");
                return eventTime + "_" + characterId;
            }
        }).reduce(new ReduceFunction<JSONObject>() {
            @Override
            public JSONObject reduce(JSONObject jsonObject, JSONObject t1) throws Exception {
                int oldValue = jsonObject.getIntValue("experience");
                int newValue = t1.getIntValue("experience");
                jsonObject.put("experience", oldValue + newValue);
                return jsonObject;
            }
        }).filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                int newValue = jsonObject.getIntValue("experience");
                return newValue > 10;
            }
        }).print().setParallelism(1);

        env.execute("WordCount from Kafka data");
    }
}

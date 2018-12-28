package com.wxmimperio.flink.connecter.kafka;

import com.alibaba.fastjson.JSONObject;
import com.wxmimperio.flink.serialization.AvroDeserializationToJson;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class KafkaSource {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000 * 60);


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.174.20.33:9092");
        //properties.setProperty("zookeeper.connect", "192.168.1.20:2181");
        properties.setProperty("group.id", "test");


        String schenaStr = "{\"type\":\"record\",\"name\":\"rtc_warning_gmys\",\"namespace\":\"com.sdo.dw.rtc\",\"doc\":\"rtc_warning_gmys\",\"fields\":[{\"name\":\"table_name\",\"type\":[\"string\",\"null\"],\"doc\":\"告警源表\",\"size\":\"0\"},{\"name\":\"character_id\",\"type\":[\"string\",\"null\"],\"doc\":\"角色ID\",\"size\":\"0\"},{\"name\":\"area_id\",\"type\":[\"int\",\"null\"],\"doc\":\"区ID\",\"size\":\"0\"},{\"name\":\"group_id\",\"type\":[\"int\",\"null\"],\"doc\":\"服ID\",\"size\":\"0\"},{\"name\":\"platform\",\"type\":[\"int\",\"null\"],\"doc\":\"平台ID\",\"size\":\"0\"},{\"name\":\"character_level\",\"type\":[\"int\",\"null\"],\"doc\":\"角色等级\",\"size\":\"0\"},{\"name\":\"warn_column\",\"type\":[\"string\",\"null\"],\"doc\":\"设置告警的字段\",\"size\":\"0\"},{\"name\":\"current_value\",\"type\":[\"string\",\"null\"],\"doc\":\"当前值\",\"size\":\"0\"},{\"name\":\"total_value\",\"type\":[\"string\",\"null\"],\"doc\":\"累计总值\",\"size\":\"0\"},{\"name\":\"event_time\",\"type\":[\"string\",\"null\"],\"doc\":\"数据源时间\",\"size\":\"0\"},{\"name\":\"warn_type\",\"type\":[\"string\",\"null\"],\"doc\":\"异常类型\",\"size\":\"0\"}],\"last_update_time\":\"Tue Dec 11 15:36:03 CST 2018\"}";
        Schema schema = new Schema.Parser().parse(schenaStr);
        FlinkKafkaConsumer011<JSONObject> myConsumer = new FlinkKafkaConsumer011<>(
                "rtc_warning_gmys",
                //ConfluentRegistryAvroDeserializationSchema.forGeneric(schema),
                new AvroDeserializationToJson(ConfluentRegistryAvroDeserializationSchema.forGeneric(schema)),
                properties
        );

        DataStream<JSONObject> stream = env.addSource(myConsumer);
        stream.map(new MapFunction<JSONObject, Object>() {
            @Override
            public Object map(JSONObject genericRecord) throws Exception {
                System.out.println(genericRecord.toString());
                return null;
            }
        });

        env.execute("WordCount from Kafka data");
    }
}

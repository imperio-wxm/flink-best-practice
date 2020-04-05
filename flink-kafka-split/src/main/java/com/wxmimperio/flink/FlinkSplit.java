package com.wxmimperio.flink;

import com.wxmimperio.flink.common.CommonUtils;
import com.wxmimperio.flink.serializable.KafkaRecordSerializationSchema;
import com.wxmimperio.flink.serializable.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.*;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;
import java.util.Properties;

public class FlinkSplit {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String servers = "192.168.1.110:9092";
        String groupId = "test";
        String sourceTopic = "src_test_wxm_split";

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", servers);
        properties.setProperty("group.id", groupId);

        DataStream<ConsumerRecord<byte[], byte[]>> stream = env.addSource(new FlinkKafkaConsumer<>(
                sourceTopic,
                new KafkaRecordDeserializationSchema(),
                properties
        ));

        stream.process(new ProcessFunction<ConsumerRecord<byte[], byte[]>, ConsumerRecord<byte[], byte[]>>() {
            @Override
            public void processElement(ConsumerRecord<byte[], byte[]> consumerRecord, Context context, Collector<ConsumerRecord<byte[], byte[]>> collector) throws Exception {
                Map<String, String> headerMap = CommonUtils.headerToMap(consumerRecord.headers());
                //System.out.println(String.format("key = %s, value = %s", new String(consumerRecord.key()), new String(consumerRecord.value())));
                System.out.println(headerMap);
            }
        }).name("split-kafka").setParallelism(2);

        stream.addSink(new FlinkKafkaProducer<>(
                CommonUtils.DEFAULT_TOPIC,
                new KafkaRecordSerializationSchema(),
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        ));

        env.execute("FlinkSplitKafka");
    }
}

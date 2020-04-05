package com.wxmimperio.flink.serializable;

import com.wxmimperio.flink.common.CommonUtils;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Map;

public class KafkaRecordSerializationSchema implements KafkaSerializationSchema<ConsumerRecord<byte[], byte[]>> {
    @Override
    public ProducerRecord<byte[], byte[]> serialize(ConsumerRecord<byte[], byte[]> element, @Nullable Long timestamp) {
        Map<String, String> headerMap = CommonUtils.headerToMap(element.headers());
        String realTopic = headerMap.containsKey(CommonUtils.REAL_TOPIC_KEY) ? headerMap.get(CommonUtils.REAL_TOPIC_KEY) : CommonUtils.DEFAULT_TOPIC;
        final ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(realTopic, element.key(), element.value());
        CommonUtils.putHeader(element, record);
        return record;
    }

}

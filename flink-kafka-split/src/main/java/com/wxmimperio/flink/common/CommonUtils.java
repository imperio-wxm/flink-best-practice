package com.wxmimperio.flink.common;

import avro.shaded.com.google.common.collect.Maps;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.util.Map;

public class CommonUtils {
    public static String REAL_TOPIC_KEY = "real_topic";
    public static String DEFAULT_TOPIC = "default_topic";

    public static Map<String, String> headerToMap(Headers headers) {
        Map<String, String> map = Maps.newHashMap();
        for (Header header : headers) {
            map.put(header.key(), new String(header.value()));
        }
        return map;
    }

    public static void putHeader(ConsumerRecord<byte[], byte[]> element, ProducerRecord<byte[], byte[]> record) {
        for (Header header : element.headers()) {
            record.headers().add(header);
        }
    }
}

package com.wxmimperio.flink.serializable;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class KafkaRecordDeserializationSchema implements KafkaDeserializationSchema<ConsumerRecord<byte[], byte[]>> {

    @Override
    public boolean isEndOfStream(ConsumerRecord<byte[], byte[]> nextElement) {
        return false;
    }

    @Override
    public ConsumerRecord<byte[], byte[]> deserialize(ConsumerRecord<byte[], byte[]> record) {
        return record;
    }

    @Override
    public TypeInformation<ConsumerRecord<byte[], byte[]>> getProducedType() {
        return TypeInformation.of(new TypeHint<ConsumerRecord<byte[], byte[]>>() {
        });
    }
}

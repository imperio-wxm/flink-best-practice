package com.wxmimperio.flink.serialization;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.formats.avro.AvroDeserializationSchema;

import java.io.IOException;

public class AvroDeserializationToJson implements DeserializationSchema<JSONObject> {
    private AvroDeserializationSchema avroDeserializationSchema;

    public AvroDeserializationToJson(AvroDeserializationSchema avroDeserializationSchema) {
        this.avroDeserializationSchema = avroDeserializationSchema;
    }

    @Override
    public JSONObject deserialize(byte[] bytes) throws IOException {
        return JSON.parseObject(avroDeserializationSchema.deserialize(bytes).toString());
    }

    @Override
    public boolean isEndOfStream(JSONObject jsonObject) {
        return false;
    }

    @Override
    public TypeInformation<JSONObject> getProducedType() {
        return TypeExtractor.getForClass(JSONObject.class);
    }
}

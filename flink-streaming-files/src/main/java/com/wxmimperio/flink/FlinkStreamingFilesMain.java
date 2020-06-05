package com.wxmimperio.flink;

import com.wxmimperio.flink.bean.Message;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;

public class FlinkStreamingFilesMain {
    public static void main(String[] args) {

        String filePath = "";

        DateTimeBucketAssigner<Message> bucketAssigner = new DateTimeBucketAssigner<Message>("yyyy-MM-dd");

        final StreamingFileSink<Message> sink = StreamingFileSink
                .forBulkFormat(new Path(filePath), ParquetAvroWriters.forReflectRecord(Message.class))
                .withNewBucketAssigner(bucketAssigner)
                .build();

    }
}

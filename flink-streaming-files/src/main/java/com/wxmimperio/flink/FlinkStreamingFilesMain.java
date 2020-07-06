package com.wxmimperio.flink;

import com.wxmimperio.flink.bean.Message;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;

public class FlinkStreamingFilesMain {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Tuple2<Long, Long>> dataStream = env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L));
        dataStream.print("src = ");

        String filePath = "/Users/weiximing/code/github/flink-best-practice/flink-streaming-files/src/output";

        DataStream<Message> msgD = dataStream.map(new MapFunction<Tuple2<Long, Long>, Message>() {
            @Override
            public Message map(Tuple2<Long, Long> t) throws Exception {
                return new Message(t.f0.toString(), (t.f0.toString() + t.f1.toString()), t.f1.toString());
            }
        });

        DateTimeBucketAssigner<Message> bucketAssigner = new DateTimeBucketAssigner<>("yyyy-MM-dd");
        final StreamingFileSink<Message> sink = StreamingFileSink
                .forBulkFormat(new Path(filePath), ParquetAvroWriters.forReflectRecord(Message.class))
                .withNewBucketAssigner(bucketAssigner)
                .build();

        msgD.addSink(sink);

        env.execute("Streaming files...");
    }
}

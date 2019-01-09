package com.wxmimperio.flink.table;

import org.apache.avro.Schema;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Avro;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.types.Row;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaConnectorTableDsl {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000 * 10);

       /* final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .createRemoteEnvironment("10.1.8.210", 8081, "D:\\d_backup\\github\\flink-best-practice\\flink-simple-demo\\target\\flink-simple-demo-1.0-SNAPSHOT.jar");
        // /home/hadoop/wxm/flink/flink-1.7.1/checkpoint
        StateBackend stateBackend = new FsStateBackend("file:///home/hadoop/wxm/flink/flink-1.7.1/checkpoint", false);
        env.enableCheckpointing(1000 * 60, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(stateBackend);*/

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.1.110:9092");
        properties.setProperty("group.id", "flink-test");
        String path = "E:\\coding\\github\\flink-best-practice\\flink-simple-demo\\src\\main\\resources\\test.avro";
        Schema schema = new Schema.Parser().parse(new FileInputStream(new File(path)));
        //Schema schema = new Schema.Parser().parse(schenaStr);

        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        tableEnv.connect(new Kafka()
                .properties(properties)
                .startFromGroupOffsets()
                .topic("stream-test1")
                .version("0.11")
        ).withSchema(new org.apache.flink.table.descriptors.Schema()
                .field("character_id", Types.STRING)
                .field("group_id", Types.INT)
                .field("level", Types.INT)
                .field("experience", Types.INT)
                .field("event_time", Types.STRING)
        ).withFormat(
                new Avro().avroSchema(schema.toString())
        ).inAppendMode().registerTableSource("stream_test1");

        // scan table
        //Table table = tableEnv.scan("rtc_warning_gmys");

        // query table
        Table table = tableEnv.sqlQuery(
                "select sum(experience) from stream_test1 where character_id = '1' and group_id = 1 group by SUBSTRING(event_time,1,10),group_id"
        );

        //   convert the Table into a retract DataStream of Row.
        //   A retract stream of type X is a DataStream<Tuple2<Boolean, X>>.
        //   The boolean field indicates the type of the change.
        //   True is INSERT, false is DELETE.
        DataStream<Tuple2<Boolean, Row>> dsRow = tableEnv.toRetractStream(table, Row.class);
        dsRow.filter(new FilterFunction<Tuple2<Boolean, Row>>() {
            @Override
            public boolean filter(Tuple2<Boolean, Row> booleanRowTuple2) throws Exception {
                if (booleanRowTuple2.f0) {
                    return (int) booleanRowTuple2.f1.getField(0) > 10;
                }
                return false;
            }
        }).print().setParallelism(1);

        env.execute("Kafka table select");
    }
}

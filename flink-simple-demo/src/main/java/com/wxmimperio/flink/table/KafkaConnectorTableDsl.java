package com.wxmimperio.flink.table;

import org.apache.avro.Schema;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Avro;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.types.Row;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

public class KafkaConnectorTableDsl {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.174.20.33:9092,10.174.20.34:9092");
        properties.setProperty("group.id", "flink-test");
        String path = "D:\\d_backup\\github\\flink-best-practice\\flink-simple-demo\\src\\main\\resources\\test.avsc";
        Schema schema = new Schema.Parser().parse(new FileInputStream(new File(path)));

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.connect(new Kafka()
                .properties(properties)
                .startFromGroupOffsets()
                .topic("test_flink1")
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

       /* ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(batchEnv);

        DataSet<BatchExe.MyData> csvInput = batchEnv.readCsvFile("D:\\d_backup\\github\\flink-best-practice\\flink-simple-demo\\src\\main\\resources\\test.csv")
                .pojoType(BatchExe.MyData.class, "characterId", "groupId");
        Table batchTable = tEnv.fromDataSet(csvInput);
        tEnv.registerTable("mydata", batchTable);

        DataSet<Row> result = tEnv.toDataSet(batchTable, Row.class);*/

        // query table
        String selectSql = "select sum(experience),character_id from stream_test1 where " +
                "group_id = 1 group by SUBSTRING(event_time,1,10),character_id";
        Table table = tableEnv.sqlQuery(selectSql);

        //   convert the Table into a retract DataStream of Row.
        //   A retract stream of type X is a DataStream<Tuple2<Boolean, X>>.
        //   The boolean field indicates the type of the change.
        //   True is INSERT, false is DELETE.
        DataStream<Tuple2<Boolean, Row>> dsRow = tableEnv.toRetractStream(table, Row.class);
        dsRow.filter((FilterFunction<Tuple2<Boolean, Row>>) booleanRowTuple2 -> {
            if (booleanRowTuple2.f0) {
                if ((int) booleanRowTuple2.f1.getField(0) > 10) {
                    return true;
                } else {
                    System.out.println(String.format("experience = %s; character_id = %s,不满足条件，不输出",
                            booleanRowTuple2.f1.getField(0),
                            booleanRowTuple2.f1.getField(1)
                    ));
                    return false;
                }
            }
            return false;
        }).print().setParallelism(1);

        env.execute("Kafka table select");

        // Exception in thread "main" org.apache.flink.table.api.TableException: Only tables that belong to this TableEnvironment can be registered.
    }
}

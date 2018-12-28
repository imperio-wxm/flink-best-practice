package com.wxmimperio.flink.table;

import org.apache.avro.Schema;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Avro;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.types.Row;

import java.util.Properties;

public class KafkaConnectorTableDsl {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StateBackend stateBackend = new FsStateBackend("file:///D:/d_backup/github/flink-best-practice/checkpoint");
        env.enableCheckpointing(1000 * 60);
        env.setStateBackend(stateBackend);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.174.20.33:9092");
        properties.setProperty("group.id", "test");
        String schenaStr = "{\"type\":\"record\",\"name\":\"rtc_warning_gmys\",\"namespace\":\"com.sdo.dw.rtc\",\"doc\":\"rtc_warning_gmys\",\"fields\":[{\"name\":\"table_name\",\"type\":[\"string\",\"null\"],\"doc\":\"告警源表\",\"size\":\"0\"},{\"name\":\"character_id\",\"type\":[\"string\",\"null\"],\"doc\":\"角色ID\",\"size\":\"0\"},{\"name\":\"area_id\",\"type\":[\"int\",\"null\"],\"doc\":\"区ID\",\"size\":\"0\"},{\"name\":\"group_id\",\"type\":[\"int\",\"null\"],\"doc\":\"服ID\",\"size\":\"0\"},{\"name\":\"platform\",\"type\":[\"int\",\"null\"],\"doc\":\"平台ID\",\"size\":\"0\"},{\"name\":\"character_level\",\"type\":[\"int\",\"null\"],\"doc\":\"角色等级\",\"size\":\"0\"},{\"name\":\"warn_column\",\"type\":[\"string\",\"null\"],\"doc\":\"设置告警的字段\",\"size\":\"0\"},{\"name\":\"current_value\",\"type\":[\"string\",\"null\"],\"doc\":\"当前值\",\"size\":\"0\"},{\"name\":\"total_value\",\"type\":[\"string\",\"null\"],\"doc\":\"累计总值\",\"size\":\"0\"},{\"name\":\"event_time\",\"type\":[\"string\",\"null\"],\"doc\":\"数据源时间\",\"size\":\"0\"},{\"name\":\"warn_type\",\"type\":[\"string\",\"null\"],\"doc\":\"异常类型\",\"size\":\"0\"}],\"last_update_time\":\"Tue Dec 11 15:36:03 CST 2018\"}";
        //Schema schema = new Schema.Parser().parse(schenaStr);

        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        tableEnv.connect(new Kafka()
                .properties(properties)
                .startFromGroupOffsets()
                .topic("rtc_warning_gmys")
                .version("0.11")
        ).withSchema(new org.apache.flink.table.descriptors.Schema()
                .field("table_name", Types.STRING)
                .field("character_id", Types.STRING)
                .field("area_id", Types.INT)
                .field("group_id", Types.INT)
                .field("platform", Types.INT)
                .field("character_level", Types.INT)
                .field("warn_column", Types.STRING)
                .field("current_value", Types.STRING)
                .field("total_value", Types.STRING)
                .field("event_time", Types.STRING)
                .field("warn_type", Types.STRING)
        ).withFormat(
                new Avro().avroSchema(schenaStr)
        ).inAppendMode().registerTableSource("rtc_warning_gmys");

        // scan table
        //Table table = tableEnv.scan("rtc_warning_gmys");

        // query table
        Table table = tableEnv.sqlQuery("select sum(area_id) from rtc_warning_gmys where area_id = 1 group by character_id,area_id,group_id,platform");

        // convert the Table into a retract DataStream of Row.
        //   A retract stream of type X is a DataStream<Tuple2<Boolean, X>>.
        //   The boolean field indicates the type of the change.
        //   True is INSERT, false is DELETE.
        DataStream<Tuple2<Boolean, Row>> dsRow = tableEnv.toRetractStream(table, Row.class);
        dsRow.map(new MapFunction<Tuple2<Boolean,Row>, Object>() {
            @Override
            public Object map(Tuple2<Boolean, Row> booleanRowTuple2) throws Exception {
                if(booleanRowTuple2.f0) {
                    System.out.println(booleanRowTuple2.f1.toString());
                    return booleanRowTuple2.f1;
                }
                return null;
            }
        });

        env.execute("Kafka table select");
    }
}

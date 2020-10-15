package com.wxmimperio.flink.kafka;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className KafkaSourceSql.java
 * @description This is the description of KafkaSourceSql.java
 * @createTime 2020-10-15 18:50:00
 */
public class KafkaSourceSql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        bsEnv.setParallelism(1);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance()
                // 启用新的blink解析
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        String kafkaSourceDDL = "CREATE TABLE `user` ("
                + "`name` STRING,"
                + "`age` INTEGER,"
                + "`timestamp` BIGINT"
                + ") WITH ("
                + "'connector.type' = 'kafka',"
                + "'connector.version' = 'universal',"
                + "'connector.topic' = 'flink_sql_kafka',"
                + "'connector.properties.bootstrap.servers' = '127.0.0.1:9092',"
                + "'connector.properties.group.id' = 'flink_sql',"
                + "'connector.startup-mode' = 'group-offsets',"
                + "'format.type' = 'json',"
                + "'update-mode' = 'append'"
                + ")";
        bsTableEnv.executeSql(kafkaSourceDDL);

        String query = "select count(age),name from `user` group by TUMBLE(CURRENT_DATE, INTERVAL '1' DAY),name";

        Table table2 = bsTableEnv.sqlQuery(query);
        bsTableEnv.toRetractStream(table2, Row.class).print();
        bsEnv.execute();
    }
}

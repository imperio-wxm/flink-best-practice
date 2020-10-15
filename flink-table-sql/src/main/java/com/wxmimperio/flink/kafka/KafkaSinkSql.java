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
 * @className KafkaSinkSql.java
 * @description This is the description of KafkaSinkSql.java
 * @createTime 2020-10-15 20:55:00
 */
public class KafkaSinkSql {

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

        String kafkaSinkDDL = "CREATE TABLE `user_count` ("
                + "`age` BIGINT,"
                + "`name` STRING"
                + ") WITH ("
                + "'connector.type' = 'kafka',"
                + "'connector.version' = 'universal',"
                + "'connector.topic' = 'flink_sql_kafka_sink',"
                + "'connector.properties.bootstrap.servers' = '127.0.0.1:9092',"
                + "'connector.properties.group.id' = 'flink_sql',"
                + "'connector.startup-mode' = 'group-offsets',"
                + "'format.type' = 'json',"
                + "'update-mode' = 'append'"
                + ")";
        bsTableEnv.executeSql(kafkaSinkDDL);

        String query = "select age,name from `user`";

        Table table2 = bsTableEnv.sqlQuery(query);
        table2.printSchema();
        table2.executeInsert("user_count");
        //bsTableEnv.toRetractStream(table2, Row.class).print();
        bsEnv.execute();
    }
}

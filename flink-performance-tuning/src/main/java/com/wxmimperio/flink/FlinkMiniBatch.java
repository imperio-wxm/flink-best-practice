package com.wxmimperio.flink;

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
 * @className FlinkMiniBatch.java
 * @description This is the description of FlinkMiniBatch.java
 * @createTime 2021-01-05 12:22:00
 */
public class FlinkMiniBatch {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        bsEnv.setParallelism(5);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance()
                // 启用新的blink解析
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        Configuration configuration = bsTableEnv.getConfig().getConfiguration();

        configuration.setString("table.exec.mini-batch.enabled", "true");
        configuration.setString("table.exec.mini-batch.allow-latency", "5 s");
        configuration.setString("table.exec.mini-batch.size", "5000");
        configuration.setString("table.optimizer.agg-phase-strategy", "TWO_PHASE");
        configuration.setString("table.optimizer.distinct-agg.split.enabled", "true");
        //configuration.setString("table.optimizer.distinct-agg.split.bucket-num","5");



        String kafkaSourceDDL = "CREATE TABLE `user` ("
                + "`name` STRING,"
                + "`age` INTEGER,"
                + "`timestamp` BIGINT,"
                + " ts as LOCALTIMESTAMP,"
                + " WATERMARK FOR `ts` as `ts` - INTERVAL '10' SECOND"
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

        //String query = "select count(name),name,TUMBLE_START(`ts`, INTERVAL '1' MINUTE) AS wk_time from `user` group by TUMBLE(`ts`, INTERVAL '1' MINUTE),name";

        String query = "select count(distinct count_num) from (select count(name) as count_num,name from `user` group by name) group by count_num";

        Table table2 = bsTableEnv.sqlQuery(query);
        bsTableEnv.toRetractStream(table2, Row.class).print();
        bsEnv.execute();
    }
}

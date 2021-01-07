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
 * @className Test.java
 * @description This is the description of Test.java
 * @createTime 2021-01-06 20:39:00
 */
public class Test {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("rest.bind-port", "8082");

        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        bsEnv.setParallelism(2);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance()
                // 启用新的blink解析
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        Configuration configuration = bsTableEnv.getConfig().getConfiguration();

        //configuration.setString("table.exec.mini-batch.enabled", "true");
        //configuration.setString("table.exec.mini-batch.allow-latency", "5 s");
        //configuration.setString("table.exec.mini-batch.size", "5000");
        //configuration.setString("table.optimizer.agg-phase-strategy", "TWO_PHASE");
        //configuration.setString("table.optimizer.distinct-agg.split.enabled", "true");
        //configuration.setString("table.optimizer.distinct-agg.split.bucket-num","5");
        //bsTableEnv.executeSql("create function precise_distinct as 'com.wxmimperio.flink.PreciseDistinct' language java");


        String kafkaSourceDDL = "CREATE TABLE `user` ("
                + "`name` STRING,"
                + "`age` BIGINT,"
                + "`timestamp` BIGINT,"
                + " ts as LOCALTIMESTAMP,"
                + " WATERMARK FOR `ts` as `ts` - INTERVAL '1' SECOND"
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

        bsTableEnv.createFunction("precise_distinct", PreciseDistinct.class);


        //String query = "select count(name) from `user` group by TUMBLE(proctime(), INTERVAL '10' SECOND)";

        //String query = "select count(distinct count_num) from (select count(name) as count_num,name from `user` group by name) group by count_num";

        //String query = "select precise_distinct(age),age from `user` group by age";

        //String query = "select precise_distinct(age),age from `user` group by age";

        String query = "select count(distinct age),age,name from `user` group by TUMBLE(proctime(), INTERVAL '10' SECOND),age,name";

        Table table2 = bsTableEnv.sqlQuery(query);
        bsTableEnv.toRetractStream(table2, Row.class).print();
        bsEnv.execute();
    }
}

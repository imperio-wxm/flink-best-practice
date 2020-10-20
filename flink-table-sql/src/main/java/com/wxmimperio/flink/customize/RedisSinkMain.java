package com.wxmimperio.flink.customize;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className RedisSinkMain.java
 * @description This is the description of RedisSinkMain.java
 * @createTime 2020-10-20 17:14:00
 */
public class RedisSinkMain {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);

        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, environmentSettings);
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);
        configuration.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(10));

        String sql = "CREATE TABLE wxm_test (`userss` BIGINT, `product` BIGINT, `amount` BIGINT) WITH ('connector'='datagen','rows-per-second'='1')";
        tableEnv.executeSql(sql);

        sql = "CREATE TABLE redis (info_index BIGINT,courier_id BIGINT,city_id BIGINT" +
                ") WITH (" +
                "'connector'='wxm-redis'," +
                "'hostPort'='xxx'," +
                "'keyType'='hash'," +
                "'keyTemplate'='test2_${city_id}'," +
                "'fieldTemplate'='test2_${courier_id}'," +
                "'valueNames'='info_index,city_id'," +
                "'expireTime'='50000')";

        tableEnv.executeSql(sql);

        Table table2 = tableEnv.sqlQuery("select * from wxm_test");

        Table resultTable = tableEnv.sqlQuery("select `userss` as info_index,`product` as courier_id,`amount` as city_id from " + table2);
        TupleTypeInfo<Tuple3<Long, Long, Long>> tupleType = new TupleTypeInfo<>(
                Types.LONG(),
                Types.LONG(),
                Types.LONG());
        tableEnv.toRetractStream(resultTable, tupleType).print("===== ");
        tableEnv.executeSql("INSERT INTO redis SELECT info_index,courier_id,city_id FROM " + resultTable);
        env.execute("");
    }
}

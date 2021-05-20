package com.wxmimperio.flink.simple;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.ApiExpression;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.Expressions.row;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className SimpleDemo.java
 * @description This is the description of SimpleDemo.java
 * @createTime 2020-10-15 16:30:00
 */
public class SimpleDemo2 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        bsEnv.setParallelism(1);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance()
                // 启用新的blink解析
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        // 废弃，用executeSql()代替
        // bsTableEnv.connect();
        // String createDDL = "";
        // bsTableEnv.executeSql(createDDL);



        String words = "hello";
        List<ApiExpression> wordList = Arrays.stream(words.split("\\W+"))
                .map(word -> row(word, 1, new Timestamp(System.currentTimeMillis())))
                .collect(Collectors.toList());

        Table table = bsTableEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("word", DataTypes.STRING().notNull()),
                        DataTypes.FIELD("frequency", DataTypes.INT().notNull()),
                        DataTypes.FIELD("time_key", DataTypes.TIMESTAMP().notNull())
                ), wordList);

        table.printSchema();

        bsTableEnv.createTemporaryView("word_count", table);

        //执行查询
        // UNIX_TIMESTAMP(string1[, string2])
        String sql = "select word, frequency, time_key /1000 from word_count";

        //sql = "select DATE_FORMAT(now(),'yyyy-MM-dd HH:mm:ss')";

        //sql = "select HOUR(TO_TIMESTAMP('2020-11-18 11:23:00'))";

        // sql = "select CHAR_LENGTH(cast(1606287574456 as varchar)) = 13";

        sql = "select * from word_count";

        Table table2 = bsTableEnv.sqlQuery(sql);
        table2.execute().print();

        // 表的标识符（catalog，dbName，tableName）
        // bsTableEnv.useCatalog("my_catalog");
        // bsTableEnv.useDatabase("my_db");
    }
}

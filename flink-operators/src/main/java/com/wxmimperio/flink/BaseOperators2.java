package com.wxmimperio.flink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

public class BaseOperators2 {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String json = "{\"action\":\"update\",\"schema\":\"bilibili_archive\",\"table\":\"archive\",\"old\":{\"access\":0,\"aid\":843675352,\"arealimit\":0,\"attribute\":16384,\"author\":\"北溟Jm\",\"click\":0,\"content\":\"昨晚公会看日出了所以没时间整，下午爆肝摸轴更新~\",\"copyright\":1,\"cover\":\"/bfs/archive/d322dd03672a5a2579ed0741af33e3072875626f.jpg\",\"ctime\":\"2021-01-13 13:12:22\",\"duration\":352,\"forward\":0,\"humanrank\":0,\"id\":172561146,\"mid\":193675051,\"mtime\":\"2021-01-13 13:45:55\",\"note\":\"\",\"pubtime\":\"2021-01-13 13:45:55\",\"reject_reason\":\"\",\"round\":0,\"src_title\":null,\"state\":0,\"tag\":\"游戏知识分享官,手机游戏,公主连结,攻略,打卡挑战\",\"title\":\"【公主连结】摩羯座B面部分高质量作业（爆肝更新中）\",\"typeid\":172},\"new\":{\"access\":0,\"aid\":843675352,\"arealimit\":0,\"attribute\":16384,\"author\":\"北溟Jm\",\"click\":0,\"content\":\"昨晚公会看日出了所以没时间整，下午爆肝摸轴更新~\",\"copyright\":1,\"cover\":\"/bfs/archive/d322dd03672a5a2579ed0741af33e3072875626f.jpg\",\"ctime\":\"2021-01-13 13:12:22\",\"duration\":352,\"forward\":0,\"humanrank\":0,\"id\":172561146,\"mid\":193675051,\"mtime\":\"2021-01-13 13:45:55\",\"note\":\"\",\"pubtime\":\"2021-01-13 13:45:55\",\"reject_reason\":\"\",\"round\":0,\"src_title\":null,\"state\":-30,\"tag\":\"游戏知识分享官,手机游戏,&gt;公主连结,攻略,打卡挑战\",\"title\":\"【公主连结】摩羯座B面部分高质量作业（爆肝更新中）\",\"typeid\":172},\"msec\":1610524751729,\"seq\":6755030408275951616,\"canal_pos_str\":\"0-4109915-5134827194\"}";


        ArrayList<Row> rowCollection = new ArrayList<>();
        rowCollection.add(Row.of("3", 3, new Timestamp(System.currentTimeMillis())));
        rowCollection.add(Row.of("5", 2, new Timestamp(System.currentTimeMillis())));

        // 根据所造数据，自己生成TypeInformation
        RowTypeInfo rowTypeInfo = getRowTypeInfo();
        DataStream<String> source = env.fromElements(json);

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        DataStream<Row> dataStream = source
                .flatMap(new FlatMapFunction<String, Row>() {
                    @Override
                    public void flatMap(String value, Collector<Row> collector) throws Exception {
                        JSONObject json = JSON.parseObject(value).getJSONObject("new");
                        //String node = json.getString("note");
                        String mtime = json.getString("mtime");

                        Row row = Row.of("test", new Timestamp(simpleDateFormat.parse(mtime).getTime()));
                        System.out.println(row + "=====");

                        collector.collect(row);
                        /*System.out.println("receive data = " + value);
                        Tuple2<Row, Row> tuple = parse(value, schema);
                        Row insertRow = tuple.f0;
                        Row deleteRow = tuple.f1;


                        if (insertRow != null) {
                            collector.collect(insertRow);
                            System.out.println("collect insertRow = " + insertRow);
                        }

                        if (deleteRow != null) {
                            collector.collect(deleteRow);
                            System.out.println("collect deleteRow = " + deleteRow);
                        }*/
                    }
                }).returns(new RowTypeInfo(Types.STRING, Types.SQL_TIMESTAMP)).keyBy(row -> row.getField(0));

        tableEnv.registerDataStream("Mytable", dataStream);

        String sql = "select * from Mytable";
        Table result = tableEnv.sqlQuery(sql);

        // 将table转换成DataStream
        DataStream<Row> resultData = tableEnv.toAppendStream(result, Row.class);

        resultData.print();

        env.execute("KeyedState");
    }

    private static RowTypeInfo getRowTypeInfo() {
        TypeInformation<Timestamp> type = TypeInformation.of(Timestamp.class);
        TypeConversions.fromLegacyInfoToDataType(type);

        TypeInformation[] types = new TypeInformation[3];
        String[] fieldNames = new String[3];

        types[0] = BasicTypeInfo.STRING_TYPE_INFO;
        types[1] = BasicTypeInfo.INT_TYPE_INFO;
        types[2] = BasicTypeInfo.of(Timestamp.class);

        fieldNames[0] = "a";
        fieldNames[1] = "b";
        fieldNames[2] = "c";
        return new RowTypeInfo(types, fieldNames);
    }
}

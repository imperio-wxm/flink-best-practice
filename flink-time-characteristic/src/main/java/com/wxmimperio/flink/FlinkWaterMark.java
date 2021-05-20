package com.wxmimperio.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;

public class FlinkWaterMark {

    /**
     * 样例数据：
     * 10000,a
     * 20000,b
     * 29999,c
     * 30000,a
     * 34998,a
     *
     * 1. 这里时间窗口是30秒，Flink的时间窗口是左闭右开的[0,30000)
     *    如果这里我们设置消息延迟是0秒，输入29999,c 就应该触发，窗口计算，可是这里我们设计的最大延迟是5秒
     *    那什么时候触发第一次窗口计算呢？应该是29999+5000=34999
     *
     * 2. 接下来我们继续输入，
     *    34999,b应该会第一次触发窗口计算进行数据的输出，至此我们应该输入了6行数据，那输出的计算结果是什么呢？
     *    这里应该是输出[0,30000)时间段之间的数据
     *
     * 3. 接下来我们还要验证两件事：
     *    一是第二次窗口触发时间，按照这样计算，那下次触发计算的的时间应该是30000+29999+5000=64999，
     *    另一个是：第一次窗口触发计算完成后，又来了一条25000，a数据，该如何处理呢
     *    由于触发了计算之后watermark应该更新成了30000，比他小的数据会被丢弃
     *
     * 4. 继续输入64999，b，触发第二次窗口计算，在[3000,59999]窗口
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置使用EventTime作为Flink的时间处理标准，不指定默认是ProcessTime
        senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 这里为了便于理解，设置并行度为1,默认并行度是当前机器的cpu数量
        senv.setParallelism(1);
        // 指定数据源 从socket的9000端口接收数据，先进行了不合法数据的过滤
        DataStream<String> sourceDs = senv.socketTextStream("127.0.0.1", 9999)
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String line) throws Exception {
                        if (null == line || "".equals(line)) {
                            return false;
                        }
                        String[] lines = line.split(",");
                        return lines.length == 2;
                    }
                });

        // 做了一个简单的map转换，将数据转换成Tuple2<long,String,Integer>格式，第一个字段代表是时间 第二个字段代表的是单词,第三个字段固定值出现了1次
        DataStream<Tuple3<Long, String, Integer>> wordDs = sourceDs.map(new MapFunction<String, Tuple3<Long, String, Integer>>() {
            @Override
            public Tuple3<Long, String, Integer> map(String line) throws Exception {
                String[] lines = line.split(",");
                return new Tuple3<>(Long.valueOf(lines[0]), lines[1], 1);
            }
        });

        // 设置Watermark的生成方式为Periodic Watermark，并实现他的两个函数getCurrentWatermark和extractTimestamp
        DataStream<Tuple3<Long, String, Integer>> wordCount = wordDs/*.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple3<Long, String, Integer>>() {
            private Long currentMaxTimestamp = 0L;
            // 最大允许的消息延迟是5秒
            private final Long maxOutOfOrderness = 5000L;

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                // 当前时间 - 最大延迟时间
                return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
            }

            @Override
            public long extractTimestamp(Tuple3<Long, String, Integer> element, long previousElementTimestamp) {
                // 从消息中抽取eventTime
                long timestamp = element.f0;
                currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                return timestamp;
            }
            // 这里根据第二个元素 单词进行统计 时间窗口是30秒 最大延时是5秒，统计每个窗口单词出现的次数
            // 时间窗口是30秒
        })*/.keyBy(1)
                .timeWindow(Time.seconds(30))
                .sum(2);

        wordCount.print("\n单词统计：");
        senv.execute("Window WordCount");
    }
}

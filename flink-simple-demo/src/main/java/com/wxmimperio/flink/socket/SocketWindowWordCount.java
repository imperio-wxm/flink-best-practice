package com.wxmimperio.flink.socket;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.SimpleFormatter;

public class SocketWindowWordCount {
    private static final Logger LOG = LoggerFactory.getLogger(SocketWindowWordCount.class);

    public static void main(String[] args) throws Exception {

        // get the execution environment
        // 本地运行
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 远程提交
        /*final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .createRemoteEnvironment("192.168.1.110",8081,"E:\\coding\\github\\flink-best-practice\\flink-simple-demo\\target\\flink-simple-demo-1.0-SNAPSHOT.jar");*/

        // get input data by connecting to the socket
        // 先启动服务端nc -l 9999
        DataStream<String> text = env.socketTextStream("10.1.8.209", 9999, "\n");

        // parse the data, group it, window it, and aggregate the counts
        final DataStream<WordWithCount> windowCounts = text
                .flatMap(new FlatMapFunction<String, WordWithCount>() {
                    @Override
                    public void flatMap(String value, Collector<WordWithCount> out) {
                        for (String word : value.split("\\s")) {
                            out.collect(new WordWithCount(word, 1L));
                        }
                    }
                })
                .keyBy("word")
                .timeWindow(Time.seconds(5), Time.seconds(5))
                .reduce(new ReduceFunction<WordWithCount>() {
                    @Override
                    public WordWithCount reduce(WordWithCount a, WordWithCount b) {
                        return new WordWithCount(a.word, a.count + b.count);
                    }
                });

        // print the results with a single thread, rather than in parallel
        final SimpleDateFormat simpleFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        windowCounts.map(new MapFunction<WordWithCount, Object>() {
            @Override
            public Object map(WordWithCount wordWithCount) throws Exception {
                LOG.info(wordWithCount.toString() + " map fun!!!");
                System.out.println(wordWithCount.toString() + " map fun!!!" + simpleFormatter.format(new Date()));
                return wordWithCount;
            }
        }).print().setParallelism(1);

        env.execute("Socket Window WordCount");
    }

    // Data type for words with count
    public static class WordWithCount {

        public String word;
        public long count;

        public WordWithCount() {
        }

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return word + " : " + count;
        }
    }
}

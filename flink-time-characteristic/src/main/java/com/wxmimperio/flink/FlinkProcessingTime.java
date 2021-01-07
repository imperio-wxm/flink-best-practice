package com.wxmimperio.flink;


import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.runtime.fs.hdfs.HadoopFsFactory;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FlinkProcessingTime {

    public static void main(String[] args) throws Exception {
        System.setProperty("java.security.krb5.conf","/Users/weiximing/code/github/flink-best-practice/flink-time-characteristic/src/main/resources/krb5.conf");

        String pathFile = "file:///department/ai/warehouse/tmp_fm_fym";
        String viewFile = "viewfs://jssz-bigdata-cluster/department/manga/warehouse/dwb_ubt_buvid_all_df_text/log_date=";

        Path path = new Path(pathFile);

        System.out.println(path.toString());

        HadoopFileSystem hadoopFileSystem = (HadoopFileSystem) new HadoopFsFactory().create(new Path(pathFile).toUri());

        org.apache.hadoop.fs.FileSystem fileSystem = hadoopFileSystem.getHadoopFileSystem();

        System.out.println(fileSystem.isFile(new Path(path.toString())));

        System.out.println(fileSystem.getConf());

        System.out.println(hadoopFileSystem.getClass().getName());


/*        org.apache.hadoop.fs.Path path = HadoopFileSystem.toHadoopPath(new Path(pathFile));

        System.out.println(HadoopFileSystem.getDefaultFsUri());
        System.out.println(path.toString());
        HadoopFileSystem fileSystem = (HadoopFileSystem) FileSystem.get(new Path(pathFile).toUri());

        System.out.println(fileSystem.getHadoopFileSystem().resolvePath(new org.apache.hadoop.fs.Path(pathFile)));*/
      /*  final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // alternatively:
        // env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple2<String, Integer>> dataStream = env
                .socketTextStream("10.1.8.210", 9999)
                .flatMap(new Splitter())
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1);

        dataStream.print();

        env.execute("Window WordCount");*/

        /*String HADOOP_CONF_PREFIX = "hadoop.conf.";

        String HADOOP_CONF_SUFFIX = "path";

        Pattern hadoopConfPath = Pattern.compile(
                Pattern.quote(HADOOP_CONF_PREFIX) +
                        // [\S&&[^.]] = intersection of non-whitespace and non-period character classes
                        "([\\S&&[^.]]*)\\." +
                        Pattern.quote(HADOOP_CONF_SUFFIX));

        Matcher matcher = hadoopConfPath.matcher("hadoop.conf.jssz-bigdata-ns1.path");
        if (matcher.matches()) {
            String hdfsHostName = matcher.group(1);
            System.out.println(hdfsHostName);
        }*/
    }

    public static void printTest(byte[] test) {

    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word : sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}

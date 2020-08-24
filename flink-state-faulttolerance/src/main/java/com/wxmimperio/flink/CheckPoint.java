package com.wxmimperio.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class CheckPoint {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        // 每 1000ms 开始一次 checkpoint
        long checkPointTime = 5000;
        env.enableCheckpointing(checkPointTime);
        // CheckpointingMode.EXACTLY_ONCE or CheckpointingMode.AT_LEAST_ONCE
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 确认 checkpoints 之间的时间会进行 500 ms
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // Checkpoint 必须在一分钟内完成，否则就会被抛弃
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 同一时间只允许一个 checkpoint 进行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 开启在 job 中止后仍然保留的 externalized checkpoints
        // Checkpoint 在默认的情况下仅用于恢复失败的作业，并不保留，当程序取消时 checkpoint 就会被删除。
        // 当然，你可以通过配置来保留 checkpoint，这些被保留的 checkpoint 在作业失败或取消时不会被清除
        // ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION：当作业取消时，保留作业的 checkpoint。注意，这种情况下，需要手动清除该作业保留的 checkpoint。
        // ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION：当作业取消时，删除作业的 checkpoint。仅当作业失败时，作业的 checkpoint 才会被保留
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 允许在有更近 savepoint 时回退到 checkpoint
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);

        // 1. MemoryStateBackend（default）
        //  在 MemoryStateBackend 内部，数据以 Java 对象的形式存储在堆中。 Key/value 形式的状态和窗口算子持有存储着状态值、触发器的 hash table。
        //  在 CheckPoint 时，State Backend 对状态进行快照，并将快照信息作为 CheckPoint 应答消息的一部分发送给 JobManager(master)，同时 JobManager 也将快照信息存储在堆内存中

        // 2. FsStateBackend
        //  FsStateBackend 将正在运行中的状态数据保存在 TaskManager 的内存中。CheckPoint 时，将状态快照写入到配置的文件系统目录中。 少量的元数据信息存储到 JobManager 的内存中

        // 3. RocksDBStateBackend
        //  RocksDBStateBackend 将正在运行中的状态数据保存在 RocksDB 数据库中，RocksDB 数据库默认将数据存储在 TaskManager 的数据目录。
        //  CheckPoint 时，整个 RocksDB 数据库被 checkpoint 到配置的文件系统目录中。 少量的元数据信息存储到 JobManager 的内存中
        //  RocksDBStateBackend 只支持异步快照，支持增量快照
        String bathPath = CheckPoint.class.getResource("/").toString();
        String path = bathPath + "checkpoint";
        String rocksPath = bathPath + "rocksdb";
        RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(path);
        rocksDBStateBackend.setDbStoragePath(rocksPath);
        rocksDBStateBackend.setNumberOfTransferThreads(1);
        rocksDBStateBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED);

        //env.setStateBackend((StateBackend) rocksDBStateBackend);

        env.setStateBackend((StateBackend) new FsStateBackend(path, true));

        DataStream<Tuple2<String, Integer>> dataStream = env
                .socketTextStream("127.0.0.1", 9999)
                .flatMap(new OperatorState.Splitter())
                .keyBy(0)
                //.timeWindow(Time.hours(1))
                .sum(1);

        dataStream.print();

        env.execute("Window WordCount");
    }
}

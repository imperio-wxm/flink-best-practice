package com.wxmimperio.flink.customize.sink;

import com.wxmimperio.flink.customize.function.RowDataPrintFunction;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className RedisSink.java
 * @description This is the description of RedisSink.java
 * @createTime 2020-10-20 16:58:00
 */
public class RedisSink implements DynamicTableSink {
    private final DataType type;
    private final ReadableConfig options;

    public RedisSink(DataType type, ReadableConfig options) {
        this.type = type;
        this.options = options;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        return changelogMode;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        DataStructureConverter converter = context.createDataStructureConverter(type);
        return SinkFunctionProvider.of(new RowDataPrintFunction(converter, options, type));
    }

    @Override
    public DynamicTableSink copy() {
        return new RedisSink(type, options);
    }

    @Override
    public String asSummaryString() {
        return "wxm-redis";
    }
}

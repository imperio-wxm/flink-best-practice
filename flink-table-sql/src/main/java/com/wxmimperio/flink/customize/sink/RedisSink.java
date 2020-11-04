package com.wxmimperio.flink.customize.sink;

import com.wxmimperio.flink.customize.function.RowDataPrintFunction;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.ROW;

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
        ChangelogMode.Builder builder = ChangelogMode.newBuilder();
        for (RowKind kind : changelogMode.getContainedKinds()) {
            if (kind != RowKind.UPDATE_BEFORE) {
                builder.addContainedKind(kind);
            }
        }
        return builder.build();
        //return changelogMode;
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

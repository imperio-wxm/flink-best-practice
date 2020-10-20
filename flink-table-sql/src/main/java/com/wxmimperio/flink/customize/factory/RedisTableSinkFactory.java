package com.wxmimperio.flink.customize.factory;

import com.wxmimperio.flink.customize.sink.RedisSink;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className RedisTableSinkFactory.java
 * @description This is the description of RedisTableSinkFactory.java
 * @createTime 2020-10-20 16:56:00
 */
public class RedisTableSinkFactory implements DynamicTableSinkFactory {
    public static final String IDENTIFIER = "wxm-redis";

    public static final ConfigOption<String> HOST_PORT = key("hostPort")
            .stringType()
            .noDefaultValue()
            .withDescription("redis host and port,");

    public static final ConfigOption<String> PASSWORD = key("password")
            .stringType()
            .noDefaultValue()
            .withDescription("redis password");

    public static final ConfigOption<Integer> EXPIRE_TIME = key("expireTime")
            .intType()
            .noDefaultValue()
            .withDescription("redis key expire time");

    public static final ConfigOption<String> KEY_TYPE = key("keyType")
            .stringType()
            .noDefaultValue()
            .withDescription("redis key type,such as hash,string and so on ");

    public static final ConfigOption<String> KEY_TEMPLATE = key("keyTemplate")
            .stringType()
            .noDefaultValue()
            .withDescription("redis key template ");

    public static final ConfigOption<String> FIELD_TEMPLATE = key("fieldTemplate")
            .stringType()
            .noDefaultValue()
            .withDescription("redis field template ");


    public static final ConfigOption<String> VALUE_NAMES = key("valueNames")
            .stringType()
            .noDefaultValue()
            .withDescription("redis value name ");

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        ReadableConfig options = helper.getOptions();
        return new RedisSink(context.getCatalogTable().getSchema().toPhysicalRowDataType(), options);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOST_PORT);
        options.add(PASSWORD);
        options.add(EXPIRE_TIME);
        options.add(KEY_TYPE);
        options.add(KEY_TEMPLATE);
        options.add(FIELD_TEMPLATE);
        options.add(VALUE_NAMES);
        return options;
    }
}

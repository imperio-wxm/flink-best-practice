package com.wxmimperio.flink.customize.function;

import com.wxmimperio.flink.customize.sink.RedisSink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.connector.sink.DynamicTableSink.DataStructureConverter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.*;

import static com.wxmimperio.flink.customize.factory.RedisTableSinkFactory.*;

/**
 * Created with IntelliJ IDEA.
 *
 * @author weiximing
 * @version 1.0.0
 * @className RowDataPrintFunction.java
 * @description This is the description of RowDataPrintFunction.java
 * @createTime 2020-10-20 17:01:00
 */
public class RowDataPrintFunction extends RichSinkFunction<RowData> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(RowDataPrintFunction.class);

    private final DataStructureConverter converter;
    private final ReadableConfig options;
    private final DataType type;
    private HashMap<String, Integer> fields;
    private JedisCluster jedisCluster;

    public RowDataPrintFunction(DataStructureConverter converter, ReadableConfig options, DataType type) {
        this.converter = converter;
        this.options = options;
        this.type = type;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        RowType logicalType = (RowType) type.getLogicalType();
        fields = new HashMap<>();
        List<RowType.RowField> rowFields = logicalType.getFields();
        int size = rowFields.size();
        for (int i = 0; i < size; i++) {
            fields.put(rowFields.get(i).getName(), i);
        }
        Set<HostAndPort> hostAndPortsSet = new HashSet<>();
        String[] hostAndPort = options.get(HOST_PORT).split(":", -1);
        //hostAndPortsSet.add(new HostAndPort(hostAndPort[0], Integer.parseInt(hostAndPort[1])));
        //jedisCluster = new JedisCluster(hostAndPortsSet);
    }

    @Override
    public void close() throws Exception {
        if (null != jedisCluster) {
            //jedisCluster.close();
        }
    }

    @Override
    public void invoke(RowData rowData, Context context) throws Exception {
        Row data = (Row) converter.toExternal(rowData);
        System.out.println("=== " + rowData + ", RowKind = " + rowData.getRowKind());
       /* RowKind rowKind = rowData.getRowKind();
        Row data = (Row) converter.toExternal(rowData);
        assert data != null;
        LOG.info(data.getField(0) + "");
        LOG.info(data + "");
        LOG.info(rowKind + "");*/
       /* if (rowKind.equals(RowKind.UPDATE_AFTER) || rowKind.equals(RowKind.INSERT)) {
            String keyTemplate = options.get(KEY_TEMPLATE);
            if (Objects.isNull(keyTemplate) || keyTemplate.trim().length() == 0) {
                throw new NullPointerException(" keyTemplate is null or keyTemplate is empty");
            }

            if (keyTemplate.contains("${")) {
                String[] split = keyTemplate.split("\\$\\{");
                keyTemplate = "";
                for (String s : split) {
                    if (s.contains("}")) {
                        String filedName = s.substring(0, s.length() - 1);
                        int index = fields.get(filedName);
                        keyTemplate = keyTemplate + data.getField(index).toString();
                    } else {
                        keyTemplate = keyTemplate + s;
                    }
                }
            }

            String keyType = options.get(KEY_TYPE);
            String valueNames = options.get(VALUE_NAMES);
            // type=hash must need fieldTemplate
            if ("hash".equalsIgnoreCase(keyType)) {
                String fieldTemplate = options.get(FIELD_TEMPLATE);
                if (fieldTemplate.contains("${")) {
                    String[] split = fieldTemplate.split("\\$\\{");
                    fieldTemplate = "";
                    for (String s : split) {
                        if (s.contains("}")) {
                            String fieldName = s.substring(0, s.length() - 1);
                            int index = fields.get(fieldName);
                            fieldTemplate = fieldTemplate + data.getField(index).toString();
                        } else {
                            fieldTemplate = fieldTemplate + s;
                        }
                    }
                }

                //fieldName = fieldTemplate-valueName
                if (valueNames.contains(",")) {
                    HashMap<String, String> map = new HashMap<>();
                    String[] fieldNames = valueNames.split(",");
                    for (String fieldName : fieldNames) {
                        String value = data.getField(fields.get(fieldName)).toString();
                        map.put(fieldTemplate + "_" + fieldName, value);
                    }
                    jedisCluster.hset(keyTemplate, map);
                } else {
                    jedisCluster.hset(keyTemplate, fieldTemplate + "_" + valueNames, data.getField(fields.get(valueNames)).toString());
                }

            } else if ("set".equalsIgnoreCase(keyType)) {
                jedisCluster.set(keyTemplate, data.getField(fields.get(valueNames)).toString());

            } else if ("sadd".equalsIgnoreCase(keyType)) {
                jedisCluster.sadd(keyTemplate, data.getField(fields.get(valueNames)).toString());
            } else if ("zadd".equalsIgnoreCase(keyType)) {
                jedisCluster.sadd(keyTemplate, data.getField(fields.get(valueNames)).toString());
            } else {
                throw new IllegalArgumentException(" not find this keyType:" + keyType);
            }

            if (Objects.nonNull(options.get(EXPIRE_TIME))) {
                jedisCluster.expire(keyTemplate, options.get(EXPIRE_TIME));
            }
        }*/
    }
}

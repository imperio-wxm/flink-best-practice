package com.wxmimperio.flink.function;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class TableSqlStateMapFunction extends RichFlatMapFunction<Tuple2<Boolean, Row>, Object> {

    private transient ValueState<Tuple2<Boolean, Row>> sum;

    @Override
    public void flatMap(Tuple2<Boolean, Row> booleanRowTuple2, Collector<Object> collector) throws Exception {
        collector.collect(booleanRowTuple2);
        sum.clear();
    }

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<Tuple2<Boolean, Row>> descriptor =
                new ValueStateDescriptor<Tuple2<Boolean, Row>>(
                        "average", // the state name
                        TypeInformation.of(new TypeHint<Tuple2<Boolean, Row>>() {}), // type information
                        Tuple2.of(false, new Row(0))); // default value of the state, if nothing was set
        sum = getRuntimeContext().getState(descriptor);
        System.out.println(getRuntimeContext().getState(descriptor) + "==========");
    }
}

package com.skhillare.wordcount.flinkjob;

import com.skhillare.wordcount.flinkjob.setqueryable.CustomSetQueryable;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class ManageState extends RichFlatMapFunction<Tuple2<String,Long>,Tuple2<String, Long> > {
    private transient ValueState<Tuple2<String,Long>> valueState_Sum;

    protected ValueState<Tuple2<String,Long>> getValueState_Sum(){
        return valueState_Sum;
    }

    protected void setValueState_Sum(ValueState<Tuple2<String,Long>> valueState_Sum) {
        this.valueState_Sum = valueState_Sum;
    }


    @Override
    public void open(Configuration config) {
        //Initialize states with state descriptors.
        ValueStateDescriptor<Tuple2<String,Long>> descriptor = CustomSetQueryable.getQueryableValueStateDescriptor();
        valueState_Sum = getRuntimeContext().getState(descriptor);
    }


    @Override
    public void flatMap(Tuple2<String, Long> input, Collector<Tuple2<String, Long>> out) throws Exception {

        //Update ValueState
        Long currentSum = valueState_Sum.value().f1;
        currentSum += Long.valueOf(input.f1);
        valueState_Sum.update(Tuple2.of(input.f0,currentSum));
        System.out.println(valueState_Sum.value());
        out.collect(valueState_Sum.value());
    }
}

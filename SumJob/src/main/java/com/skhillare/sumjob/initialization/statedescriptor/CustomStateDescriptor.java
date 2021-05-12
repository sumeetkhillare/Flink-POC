package com.skhillare.sumjob.initialization.statedescriptor;

import com.skhillare.sumjob.manageproperties.ManageFixedProperties;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;

public class CustomStateDescriptor {
    public static ManageFixedProperties prop = new ManageFixedProperties();
    //Initialize State Descriptors, Names for state decriptors are from fixedvalues.properties
    public static ValueStateDescriptor<Long> getValueStateDescriptor(){
        return new ValueStateDescriptor<>(
                prop.getValue_Q_NAME(), // the state name
                BasicTypeInfo.LONG_TYPE_INFO, // type information
                0L);
    }

    public static ValueStateDescriptor<Tuple2<String,Long>> getValueStateTupleDescriptor(){
        return new ValueStateDescriptor<>(
                prop.getValue_Q_NAME(), // the state name
                TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}), // type information
                Tuple2.of(prop.getStatekey(), 0L));
    }
    public static ListStateDescriptor<Long> getListStateDescriptor(){
        return  new ListStateDescriptor<>(prop.getList_Value_Q_NAME(), BasicTypeInfo.LONG_TYPE_INFO);
    }
    public static MapStateDescriptor<String, Long> getMapStateDescriptor(){
        return new MapStateDescriptor<>(
                prop.getMap_Value_Q_NAME(),
                String.class,
                Long.class);
    }
    public static ReducingStateDescriptor<Long> getReducingStateDescriptor(){
        return new ReducingStateDescriptor<>(
                prop.getReduce_Value_Q_NAME(),
                new ReduceFunction<Long>() {
                    @Override
                    public Long reduce(Long v1, Long v2) {
                        return (v1+v2);
                    }
                },BasicTypeInfo.LONG_TYPE_INFO);
    }
}
package com.skhillare.wordcount.initialization.statedescriptor;

import com.skhillare.wordcount.manageproperties.ManageFixedProperties;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;

public class CustomStateDescriptor {
    public static ManageFixedProperties prop = new ManageFixedProperties();
    //Initialize State Descriptors, Names for state decriptors are from fixedvalues.properties
    public static ValueStateDescriptor<Tuple2<String,Long>> getValueStateDescriptor(){
        return new ValueStateDescriptor<>(
                prop.getValue_Q_NAME(), // the state name
                TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}), // type information
                Tuple2.of("",0L));
    }
}

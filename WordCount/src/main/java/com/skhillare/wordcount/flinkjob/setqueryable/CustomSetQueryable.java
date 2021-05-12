package com.skhillare.wordcount.flinkjob.setqueryable;

import com.skhillare.wordcount.manageproperties.ManageFixedProperties;
import com.skhillare.wordcount.initialization.statedescriptor.CustomStateDescriptor;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.tuple.Tuple2;

public class CustomSetQueryable {
    public static ManageFixedProperties prop = new ManageFixedProperties();

    //Set State Descriptors Queryable, Name for each queryable state is from fixedvalues.properties
    public static ValueStateDescriptor<Tuple2<String,Long>> getQueryableValueStateDescriptor(){
        ValueStateDescriptor<Tuple2<String,Long>> valueStateDescriptor= CustomStateDescriptor.getValueStateDescriptor();
        valueStateDescriptor.setQueryable(prop.getValue_Q_NAME());
        return valueStateDescriptor;
    }
}

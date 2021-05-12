package com.skhillare.sumjob.flinkjob.setqueryable;

import com.skhillare.sumjob.manageproperties.ManageFixedProperties;
import com.skhillare.sumjob.initialization.statedescriptor.CustomStateDescriptor;
import org.apache.flink.api.common.state.*;

public class CustomSetQueryable {
    public static ManageFixedProperties prop = new ManageFixedProperties();

    //Set State Descriptors Queryable, Name for each queryable state is from fixedvalues.properties
    public static ValueStateDescriptor<Long> getQueryableValueStateDescriptor(){
        ValueStateDescriptor<Long> valueStateDescriptor= CustomStateDescriptor.getValueStateDescriptor();
        valueStateDescriptor.setQueryable(prop.getValue_Q_NAME());
        return valueStateDescriptor;
    }

    public static ListStateDescriptor<Long> getQueryableListStateDescriptor(){
        ListStateDescriptor<Long> listStateDescriptor=CustomStateDescriptor.getListStateDescriptor();
        listStateDescriptor.setQueryable(prop.getList_Value_Q_NAME());
        return listStateDescriptor;
    }

    public static MapStateDescriptor<String, Long> getQueryableMapStateDescriptor(){
        MapStateDescriptor<String,Long> mapStateDescriptor=CustomStateDescriptor.getMapStateDescriptor();
        mapStateDescriptor.setQueryable(prop.getMap_Value_Q_NAME());
        return mapStateDescriptor;
    }

    public static ReducingStateDescriptor<Long> getQueryableReducingStateDescriptor(){
        ReducingStateDescriptor<Long> reducingStateDescriptor=CustomStateDescriptor.getReducingStateDescriptor();
        reducingStateDescriptor.setQueryable(prop.getReduce_Value_Q_NAME());
        return reducingStateDescriptor;
    }
}

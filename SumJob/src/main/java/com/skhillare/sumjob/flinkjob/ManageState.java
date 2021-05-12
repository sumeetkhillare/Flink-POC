package com.skhillare.sumjob.flinkjob;

import com.skhillare.sumjob.flinkjob.setqueryable.CustomSetQueryable;
import com.skhillare.sumjob.manageproperties.ManageFixedProperties;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.flink.util.Collector;
import java.util.ArrayList;
import java.util.Collections;

public class ManageState extends RichFlatMapFunction<Tuple2<String, Long>,Tuple2<String, Long> > {
    protected static ManageFixedProperties prop=new ManageFixedProperties();
    private transient ValueState<Long> valueState_Sum;
    private transient ListState<Long> listState_Sum;
    private transient MapState<String, Long> mapState_Sum;
    private transient ReducingState<Long> reducingState_Sum;
    protected final static String statekey = prop.getStatekey();
    protected ListState<Long> getListState_Sum() {
        return listState_Sum;
    }

    protected MapState<String, Long> getMapState_Sum() {
        return mapState_Sum;
    }

    protected ReducingState<Long> getReducingState_Sum() {
        return reducingState_Sum;
    }

    protected ValueState<Long> getValueState_Sum(){
        return valueState_Sum;
    }

    protected void setValueState_Sum(ValueState<Long> valueState_Sum) {
        this.valueState_Sum = valueState_Sum;
    }

    protected void setListState_Sum(ListState<Long> listState_Sum) {
        this.listState_Sum = listState_Sum;
    }

    protected void setMapState_Sum(MapState<String, Long> mapState_Sum) {
        this.mapState_Sum = mapState_Sum;
    }

    protected void setReducingState_Sum(ReducingState<Long> reducingState_Sum) {
        this.reducingState_Sum = reducingState_Sum;
    }


    @Override
    public void open(Configuration config) {
        //Initialize states with state descriptors.
        ValueStateDescriptor<Long> descriptor = CustomSetQueryable.getQueryableValueStateDescriptor();
        valueState_Sum = getRuntimeContext().getState(descriptor);
        ListStateDescriptor<Long> listDis = CustomSetQueryable.getQueryableListStateDescriptor();
        listState_Sum = getRuntimeContext().getListState(listDis);
        MapStateDescriptor<String, Long> mapStateDes = CustomSetQueryable.getQueryableMapStateDescriptor();
        mapState_Sum = getRuntimeContext().getMapState(mapStateDes);
        ReducingStateDescriptor<Long> reducingStateDescriptor = CustomSetQueryable.getQueryableReducingStateDescriptor();
        reducingState_Sum=getRuntimeContext().getReducingState(reducingStateDescriptor);
    }

    protected void clearStates(){
        //Clear all states
        valueState_Sum.clear();
        listState_Sum.clear();
        mapState_Sum.clear();
        reducingState_Sum.clear();
    }
    private void printListstate() throws Exception{
        //Print ListState
        ArrayList<Long> listdata = Lists.newArrayList(listState_Sum.get());
        System.out.println("ListState : ");
        for (Long data : listdata) {
            System.out.print(data.toString()+", ");
        }
        System.out.print("\n");
    }

    protected void printStates() throws Exception {
        //Print state data
        System.out.println("**************\nPrinting States");
        System.out.println("ValueState : "+(valueState_Sum.value()));
        printListstate();
        System.out.println("MapState : "+mapState_Sum.get(statekey).toString());
        System.out.println("ReducingState : "+reducingState_Sum.get().toString()+"\n" );
     }


    @Override
    public void flatMap(Tuple2<String, Long> input, Collector<Tuple2<String, Long>> out) throws Exception {

        if (input.f1==-1){
            clearStates();
            return;
        }
        //Update ValueState
        Long currentSum = valueState_Sum.value();
        currentSum += input.f1;
        valueState_Sum.update(currentSum);
        //Update ListState
        Iterable<Long> state = listState_Sum.get();
        if (null == state) {
            listState_Sum.addAll(Collections.EMPTY_LIST);
        }
        listState_Sum.add(input.f1);
        //Update MapState
        mapState_Sum.put(statekey,valueState_Sum.value());
        //Update RedcingState
        reducingState_Sum.add(input.f1);
        printStates();
        out.collect(Tuple2.of(statekey,valueState_Sum.value()));
    }
}

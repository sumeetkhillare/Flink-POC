/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.queryablestatedemo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.FlinkMiniCluster;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;

class QuitValueState extends Exception{
    QuitValueState(String m1,String inetAddress,int port) throws IOException {
        super(m1);
        Socket socket = new Socket();
        SocketAddress socketAddress=new InetSocketAddress(inetAddress, port);
        socket.bind(socketAddress);
        socket.close();

    }
}


public class FlinkJob extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<String, String>> {

    @Override
    public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<String, String>> out) throws Exception {
        if (input.f1==-1){
            sum.clear();
            return;
        }
        Tuple2<Long, Long> currentSum = sum.value();
        currentSum.f0 += 1;
        currentSum.f1 += input.f1;
        //Throw arithmatic exception for checkpoint restarting
        if (input.f1==155){
            throw new ArithmeticException("not valid");
        }

        sum.update(currentSum);
        System.out.println("Current Sum: "+(sum.value().f1)+"\nCurrent Count: "+(sum.value().f0));
        if (sum.value().f0>=2) {
            double avg=(Double.valueOf(sum.value().f1) / Double.valueOf(sum.value().f0));
            out.collect(new Tuple2<>(String.valueOf(avg),"avg"));
        }
    }
    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
                new ValueStateDescriptor<>(
                        "average", // the state name
                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}), // type information
                        Tuple2.of(0L, 0L)); // default value of the state, if nothing was set
        sum = getRuntimeContext().getState(descriptor);
    }
    public final static String QUERY_NAME = "average-query";
    public transient ValueState<Tuple2<Long, Long>> sum;
    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        final int parallelism = params.getInt("parallelism", 1);

        Configuration config = new Configuration();
        config.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, 6124);
        config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1);
        config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, parallelism);
        // In a non MiniCluster setup queryable state is enabled by default.
        config.setBoolean(QueryableStateOptions.SERVER_ENABLE, true);

        FlinkMiniCluster flinkCluster = new LocalFlinkMiniCluster(config, false);
        try {
            flinkCluster.start(true);

            StreamExecutionEnvironment env = StreamExecutionEnvironment
                    .createRemoteEnvironment("localhost", 6124, parallelism);

            final String hostname="localhost";
            DataStreamSource<String> inp = env.socketTextStream(hostname, 9000, "\n");

            inp.flatMap(new FlatMapFunction<String, Tuple2<Long, Long>>() {
                @Override
                public void flatMap(String inpstr, Collector<Tuple2<Long, Long>> out) throws Exception{

                    for (String word : inpstr.split("\\s")) {
                        try {
                            if(word.equals("quit")){
                                throw new QuitValueState( "Stoppping!!!",hostname,9000);
                            }
                            if(word.equals("clear")){
                                word="-1";
                            }
                            out.collect(Tuple2.of(1L, Long.valueOf(word)));
                        }
                        catch ( NumberFormatException e) {
                            System.out.println("Enter valid number: "+e.getMessage());
                        }catch (QuitValueState ex){
                            System.out.println("Quitting!!!");
                        }
                    }
                }
            }).keyBy(0).flatMap(new FlinkJob())
                    .keyBy(1)
                    .asQueryableState(QUERY_NAME);

            JobGraph jobGraph = env.getStreamGraph().getJobGraph();

            System.out.println("Submitting Job");
            System.out.println();
            env.execute();
            flinkCluster.submitJobAndWait(jobGraph, false);
        } finally {
            flinkCluster.shutdown();
        }
    }

}

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


//--port 9000 --checkpoint 5000
//-c com.skhillare.flink.StreamingJob

package com.skhillare.flink;
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


//--port 9000 --checkpoint 5000
//-c com.skhillare.flink_avg.StreamingJob
//./bin/flink run -c com.skhillare.flink.StreamingJob ~/flink-quick-1.0-SNAPSHOT.jar --port 9000 --checkpoint 5000

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.time.Time;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

//@SuppressWarnings("serial")
class QuitValueState extends Exception{
	QuitValueState(String m1,String inetAddress,int port) throws IOException {
		super(m1);
		Socket socket = new Socket();
		SocketAddress socketAddress=new InetSocketAddress(inetAddress, port);
		socket.bind(socketAddress);
		socket.close();

	}
}



public class StreamingJob extends RichFlatMapFunction<Tuple2<String, Long>, Tuple2<String, Long>> {
	public final static String Q_NAME = "query";
	public transient ValueState<Tuple2<String, Long>> sum;

	@Override
	public void flatMap(Tuple2<String, Long> input, Collector<Tuple2<String, Long>> out) throws Exception {
		Tuple2<String, Long> currentSum = sum.value();
		if (input.f1==-1){
			sum.clear();
			return;
		}
		currentSum.f1 += input.f1;
		sum.update(currentSum);
		System.out.println("Current Sum: "+(sum.value().f1)+"\nCurrent Count: "+(sum.value().f0));
		out.collect(new Tuple2<>(Q_NAME, sum.value().f1));
	}
	@Override
	public void open(Configuration config) {
		ValueStateDescriptor<Tuple2<String, Long>> descriptor =
				new ValueStateDescriptor<>(
						Q_NAME, // the state name
						TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}), // type information
						Tuple2.of(Q_NAME, 0L)); // default value of the state, if nothing was set
//		descriptor.setQueryable("query-name");
		sum = getRuntimeContext().getState(descriptor);
	}
	public static void main(String[] args) throws Exception {
		final String hostname="localhost";
		final Integer port=9000;
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		ValueStateDescriptor<Tuple2<String, Long>> descriptor =
				new ValueStateDescriptor<>(
						Q_NAME, // the state name
						TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}), // type information
						Tuple2.of("avg", 0L));

		DataStreamSource<String> inp = env.socketTextStream(hostname, port, "\n");

		inp.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
			@Override
			public void flatMap(String inpstr, Collector<Tuple2<String, Long>> out) throws Exception{

				for (String word : inpstr.split("\\s")) {
					try {
						if(word.equals("quit")){
							throw new QuitValueState( "Stoppping!!!",hostname,port);
						}
						if(word.equals("clear")){
							word="-1";
						}
						out.collect(Tuple2.of(Q_NAME, Long.valueOf(word)));
					}
					catch ( NumberFormatException e) {
						System.out.println("Enter valid number: "+e.getMessage());
					}catch (QuitValueState ex){
						System.out.println("Quitting!!!");
					}
				}
			}
		}).name("Input Conversion").keyBy(0).flatMap(new StreamingJob()).name("Store in State").keyBy(0)
				.asQueryableState(Q_NAME,descriptor);
		JobGraph jobGraph = env.getStreamGraph().getJobGraph();
//		System.out.println("[info] Job ID: " + jobGraph.getJobID());
		System.out.println("Running!!!");
		env.execute("Sum Job");
//		flinkCluster.submitJobAndWait(jobGraph, false);
	}


}
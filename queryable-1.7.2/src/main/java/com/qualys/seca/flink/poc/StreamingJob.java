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

package com.qualys.seca.flink.poc;
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

//./bin/flink run -c com.qualys.seca.flink.poc.StreamingJob ~/flink-quick-1.0-SNAPSHOT.jar --port 9000 --checkpoint 5000

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

//Custom Exception to Quit
class QuitValueState extends Exception{
	QuitValueState(String m1,String inetAddress,int port) throws IOException {
		super(m1);
		Socket socket = new Socket();
		SocketAddress socketAddress=new InetSocketAddress(inetAddress, port);
		socket.bind(socketAddress);
		socket.close();

	}
}



public class StreamingJob extends RichFlatMapFunction<Tuple2<String, Long>, Tuple2<String, Long>>  {
	//StateKey Common for all states.
	public final static String statekey = "statekey";
	//ValueState
	public final static String Value_Q_NAME = "value-query";
	public transient ValueState<Tuple2<String, Long>> sum;
	//ListState
	public final static String List_Value_Q_NAME = "list-query";
	private ListState<Tuple2<String, Long>> listState;
	//MapState
	private transient MapState<String, Long> mapState;
	public final static String Map_Value_Q_NAME = "map-query";
	//ReducingState
	private transient ReducingState<Tuple2<String,Long>> reducingState;
	public final static String Reduce_Value_Q_NAME = "reduce-query";
	//For flatmap operator
	public final static String Keyed_Value_Q_NAME = "keyed-query";

	//Function to print ListState
	private void printstate() throws Exception{
		ArrayList<Tuple2<String, Long>> listdata = Lists.newArrayList(listState.get());
		System.out.println("ListState : ");
		for (Tuple2<String, Long> data : listdata) {
			System.out.print(data.f1.toString()+" ");
		}
		System.out.print("\n");

	}

	//Flatmap function to update values in states.
	@Override
	public void flatMap(Tuple2<String, Long> input, Collector<Tuple2<String, Long>> out) throws Exception {
		//clear state
		if (input.f1==-1){
			sum.clear();
			listState.clear();
			mapState.clear();
			reducingState.clear();
			return;
		}
		//ValueState
		Tuple2<String, Long> currentSum = sum.value();
		currentSum.f1 += input.f1;
		sum.update(currentSum);
		System.out.println("**************\nValueState : "+(sum.value().f1));
		//ListState
		Iterable<Tuple2<String, Long>> state = listState.get();
		if (null == state) {
			listState.addAll(Collections.EMPTY_LIST);
		}
		listState.add(input);
		printstate();
		//MapState
		mapState.put(statekey,sum.value().f1);
		System.out.println("MapState : "+mapState.get(statekey).toString());
		//ReducingState
		reducingState.add(Tuple2.of(statekey,input.f1));
		System.out.println("ReducingState : "+reducingState.get().toString() );
		//Collect for flatmap to query.
		out.collect(new Tuple2<>(statekey, sum.value().f1));

	}
	//Initialize States
	@Override
	public void open(Configuration config) {
		//ValueState
		ValueStateDescriptor<Tuple2<String, Long>> descriptor =
				new ValueStateDescriptor<>(
						Value_Q_NAME,
						TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}),
						Tuple2.of(statekey, 0L));
		descriptor.setQueryable(Value_Q_NAME);
		sum = getRuntimeContext().getState(descriptor);
		//ListState
		ListStateDescriptor<Tuple2<String, Long>> listDis = new ListStateDescriptor<>(List_Value_Q_NAME, TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
		}));
		listDis.setQueryable(List_Value_Q_NAME);
		listState = getRuntimeContext().getListState(listDis);
		//MapState
		MapStateDescriptor<String, Long> mapStateDes = new MapStateDescriptor<>(
				Map_Value_Q_NAME,
				String.class,
				Long.class);
		mapStateDes.setQueryable(Map_Value_Q_NAME);
		mapState = getRuntimeContext().getMapState(mapStateDes);
		//ReducingState
		ReducingStateDescriptor<Tuple2<String,Long>> reducingStateDescriptor = new ReducingStateDescriptor<>(
				Reduce_Value_Q_NAME,
				new ReduceFunction<Tuple2<String, Long>>() {
					@Override
					public Tuple2<String, Long> reduce(Tuple2<String, Long> stringLongTuple2, Tuple2<String, Long> t1) {
						return Tuple2.of(statekey,stringLongTuple2.f1+t1.f1);
					}
				},TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {})
		);
		reducingStateDescriptor.setQueryable(Reduce_Value_Q_NAME);
		reducingState=getRuntimeContext().getReducingState(reducingStateDescriptor);

	}
	public static void main(String[] args) throws Exception {
		final String hostname;
		final Integer port;
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		//Get Input i.e. hostname,port,time for checkpointing
		try {
			final ParameterTool params = ParameterTool.fromArgs(args);
			hostname = params.has("hostname") ? params.get("hostname") : "localhost";
			port = params.getInt("port");
			long cpInterval = params.getLong("checkpoint", TimeUnit.MINUTES.toMillis(1));
			if (cpInterval > 0) {
				CheckpointConfig checkpointConf = env.getCheckpointConfig();
				checkpointConf.setCheckpointInterval(cpInterval);
				checkpointConf.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
				checkpointConf.setCheckpointTimeout(TimeUnit.HOURS.toMillis(1));
				checkpointConf.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
				env.getConfig().setUseSnapshotCompression(true);
			}
		}catch (Exception e) {
			System.err.println(
					"No port specified. Please run 'SocketWindowWordCount "
							+ "--hostname <hostname> --port <port> --checkpoint <time>', where hostname (localhost by default) "
							+ "and port is the address of the text server "+ "and chekpoint is the time for checkpointing");
			System.err.println(
					"To start a simple server, run 'netcat -l <port>' and "
							+ "type the numbers into the command line");
			return;
		}

		DataStreamSource<String> inp = env.socketTextStream(hostname, port, "\n");
		//ValueState descriptor used for query flatmap
		ValueStateDescriptor<Tuple2<String, Long>> descriptor =
				new ValueStateDescriptor<>(
						Keyed_Value_Q_NAME, // the state name
						TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}), // type information
						Tuple2.of(statekey, 0L));

		//Get streaming data
		inp.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
			@Override
			public void flatMap(String inpstr, Collector<Tuple2<String, Long>> out) throws Exception{
				//Split numbers from string
				for (String word : inpstr.split("\\s")) {
					try {
						if(word.equals("quit")){
							throw new QuitValueState( "Stoppping!!!",hostname,port);
						}
						if(word.equals("clear")){
							word="-1";
						}
						//Collect statekey and number
						out.collect(Tuple2.of(statekey, Long.valueOf(word)));
					}
					//Input is not number
					catch ( NumberFormatException e) {
						System.out.println("Enter valid number: "+e.getMessage());
					}
					//Quit
					catch (QuitValueState ex){
						System.out.println("Quitting!!!");
					}
				}
			}
		}).name("Flatmap for input conversion").keyBy(0).flatMap(new StreamingJob()).name("Flatmap to store in state")
				.keyBy(0)
				//Set flatmap keyed stream Queryable.
				.asQueryableState(Keyed_Value_Q_NAME,descriptor);
		System.out.println("Sum Calculation Job Running!!!");
		env.execute("Sum Job");

	}

}
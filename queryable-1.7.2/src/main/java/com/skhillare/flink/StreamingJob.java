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
import org.apache.flink.api.common.state.*;
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
import scala.util.parsing.json.JSONObject;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
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



public class StreamingJob extends RichFlatMapFunction<Tuple2<String, Long>, Tuple2<String, Long>>  {
	public final static String Q_NAME = "query";
	public transient ValueState<Tuple2<String, Long>> sum;
	public final static String List_Q_NAME = "list-query";
	private ListState<Tuple2<String, Long>> listState;
	public final static String statekey = "statekey";
	private transient MapState<String, Long> mapState;
	public final static String Map_Q_NAME = "map-query";

	private void printstate() throws Exception{
		ArrayList<Tuple2<String, Long>> listdata = Lists.newArrayList(listState.get());
		System.out.println("liststate data");
		for (Tuple2<String, Long> data : listdata) {
			System.out.print(data.f1.toString()+" ");
		}
		System.out.print("\n");

	}
	@Override
	public void flatMap(Tuple2<String, Long> input, Collector<Tuple2<String, Long>> out) throws Exception {
		Iterable<Tuple2<String, Long>> state = listState.get();
		if (null == state) {
			listState.addAll(Collections.EMPTY_LIST);
		}
		if (input.f1==-1){
			listState.clear();
			sum.clear();
			mapState.clear();
			return;
		}
		listState.add(input);
		printstate();

		Tuple2<String, Long> currentSum = sum.value();
		currentSum.f1 += input.f1;
		sum.update(currentSum);
		System.out.println("Current Sum: "+(sum.value().f1));
		mapState.put(statekey,sum.value().f1);
		System.out.println("MapState:");
		System.out.println(String.valueOf(mapState.get(statekey)));

//		out.collect(new Tuple2<>(Q_NAME, sum.value().f1));

	}
	@Override
	public void open(Configuration config) {
		ValueStateDescriptor<Tuple2<String, Long>> descriptor =
				new ValueStateDescriptor<>(
						Q_NAME, // the state name
						TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}), // type information
						Tuple2.of(statekey, 0L)); // default value of the state, if nothing was set
		descriptor.setQueryable(Q_NAME);
		sum = getRuntimeContext().getState(descriptor);

		ListStateDescriptor<Tuple2<String, Long>> listDis = new ListStateDescriptor<>(List_Q_NAME, TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
		}));
		listDis.setQueryable(List_Q_NAME);
		listState = getRuntimeContext().getListState(listDis);

		MapStateDescriptor<String, Long> mapStateDes = new MapStateDescriptor<>(
				Map_Q_NAME,
				String.class,
				Long.class);
		mapStateDes.setQueryable(Map_Q_NAME);
		mapState = getRuntimeContext().getMapState(mapStateDes);


	}
	public static void main(String[] args) throws Exception {
		final String hostname;
		final Integer port;
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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
					"To start a simple text server, run 'netcat -l <port>' and "
							+ "type the input text into the command line");
			return;
		}


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

						out.collect(Tuple2.of(statekey, Long.valueOf(word)));
					}
					catch ( NumberFormatException e) {
						System.out.println("Enter valid number: "+e.getMessage());
					}catch (QuitValueState ex){
						System.out.println("Quitting!!!");
					}
				}
			}
		}).name("Input Conversion").keyBy(0).flatMap(new StreamingJob()).name("Store in State");
//				.keyBy(0)
//				.asQueryableState(Q_NAME,descriptor);
		JobGraph jobGraph = env.getStreamGraph().getJobGraph();
//		System.out.println("[info] Job ID: " + jobGraph.getJobID());
		System.out.println("Running!!!");
		env.execute("Sum Job");
	}


}
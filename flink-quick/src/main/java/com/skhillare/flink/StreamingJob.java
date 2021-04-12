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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */

class Quit extends Exception{
	Quit(String m1,String inetAddress,int port) throws IOException {
		super(m1);
		Socket socket = new Socket();
		SocketAddress socketAddress=new InetSocketAddress(inetAddress, port);
		socket.bind(socketAddress);
		socket.close();

	}
}



@SuppressWarnings("serial")
public class StreamingJob extends RichFlatMapFunction<Tuple2<Long, String>, Tuple2<Long, String>> {
	private ListState<Tuple2<Long, String>> listState;

	private void printstate() throws Exception{
		ArrayList<Tuple2<Long, String>> listdata = Lists.newArrayList(listState.get());
		System.out.println("\nliststate data");
		for (Tuple2<Long, String> data : listdata) {
			System.out.println(data.f1.toString()+" "+data.f0.toString() );
		}
		System.out.print("\n");

	}
	@Override
	public void flatMap(Tuple2<Long, String> input, Collector<Tuple2<Long, String>> out) throws Exception {
		Iterable<Tuple2<Long, String>> state = listState.get();
		if (null == state) {
			listState.addAll(Collections.EMPTY_LIST);
		}
		ArrayList<Tuple2<Long, String>> listdata = Lists.newArrayList(listState.get());

		Long count = 0L;
			for (Tuple2<Long, String> data : listdata) {

				if(input.f1.equals("clearstate")){
					listState.clear();
					listdata.clear();
					return;
				}else
				if(data.f1.equals(input.f1)){
					data.f0=data.f0+1;
					break;
				}
				count++;
			}
			if(count==listdata.size()){
				listdata.add(input);
			}
			listState.update(listdata);

			out.collect(Tuple2.of(input.f0, input.f1));

		printstate();
	}

	/** Data type for words with count. */

	@Override
	public void open(Configuration parameters) throws Exception {

		ListStateDescriptor<Tuple2<Long, String>> listDis = new ListStateDescriptor<>("wc", TypeInformation.of(new TypeHint<Tuple2<Long, String>>() {
		}));

		listState = getRuntimeContext().getListState(listDis);
	}


	public static void main(String[] args) throws Exception {
		final String CLASS_NAME = StreamingJob.class.getSimpleName();

		// the host and the port to connect to
		final String hostname;
		final int port;
		// get the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		try {
			final ParameterTool params = ParameterTool.fromArgs(args);
//			hostname="localhost";
//			port=9000;
			hostname = params.has("hostname") ? params.get("hostname") : "localhost";
			port = params.getInt("port");
			// Configure checkpointing if interval is set
			long cpInterval = params.getLong("checkpoint", TimeUnit.MINUTES.toMillis(1));
			if (cpInterval > 0) {
				CheckpointConfig checkpointConf = env.getCheckpointConfig();
				checkpointConf.setCheckpointInterval(cpInterval);
				checkpointConf.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
				checkpointConf.setCheckpointTimeout(TimeUnit.HOURS.toMillis(1));
				checkpointConf.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
				env.getConfig().setUseSnapshotCompression(true);

			}


		} catch (Exception e) {
			System.err.println(
					"No port specified. Please run 'SocketWindowWordCount "
							+ "--hostname <hostname> --port <port>', where hostname (localhost by default) "
							+ "and port is the address of the text server");
			System.err.println(
					"To start a simple text server, run 'netcat -l <port>' and "
							+ "type the input text into the command line");
			return;
		}

		//restart attempts after time delay
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,10000));

		//restart attempts in fixed time
//		env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.of(5, TimeUnit.MINUTES),Time.of(10, TimeUnit.SECONDS)	));




		// get input data by connecting to the socket
		DataStream<String> text = env.socketTextStream(hostname, port, "\n");
		DataStream<Tuple2<Long,String>> windowCounts=text.flatMap(new FlatMapFunction<String, Tuple2<Long, String>>() {
			@Override
			public void flatMap(String inpstr, Collector<Tuple2<Long, String>> out) throws Exception{
				for (String word : inpstr.split("\\s")) {
					try {
						if(word.equals("quit")){
							throw new Quit( "Stoppping!!!",hostname,port);
						}
						out.collect(Tuple2.of(1L, (word)));
					}catch (Quit ex){
						System.out.println("Quitting!!!");
					}
				}
			}
		}).keyBy(0).flatMap(new StreamingJob());
		// print the results with a single thread, rather than in parallel
		System.out.println("Starting on "+hostname+" port: "+String.valueOf(port)+"\n");
		System.out.println("Use clearstate to clear and quit to exit!\n");
		env.execute("Wordcount!");

	}

}
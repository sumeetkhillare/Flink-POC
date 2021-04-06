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

package com.skhillare.flink_avg;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


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

@SuppressWarnings("serial")
public class StreamingJob {

	public static class AverageClass {
		long key;
		long count;
		long sum;

		public AverageClass(long key,long count,long sum){
			this.count=count;
			this.sum=sum;
			this.key=key;
		}
		@Override
		public String toString() {
			float fsum=this.sum;
			float fcount=this.count;
			return "Average: " + fsum/fcount;
		}

	}

	public static class AverageClassTwoValue{
		long key;
		double sum;

		public AverageClassTwoValue(long key,double sum){
			this.key=key;
			this.sum=sum;
		}
		@Override
		public String toString() {
			return "Average: " + sum;
		}
	}

	public static void main(String[] args) throws Exception {
		final long[] numbercount = {1};
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
		DataStreamSource<String> inp = env.socketTextStream(hostname, port, "\n");



		//calculates average at final step
//		DataStream<AverageClass> windowCounts =
//				inp.flatMap(
//						new FlatMapFunction<String, AverageClass>() {
//							@Override
//							public void flatMap(
//									String value, Collector<AverageClass> out) {
//								for (String word : value.split("\\s")) {
//									try {
//										out.collect(new AverageClass(1L, 0L, Long.valueOf(word)));
//									}
//									catch ( NumberFormatException e) {
//										System.out.println("Enter valid number: "+e.getMessage());
//									}
//								}
//							}
//
//
//						})
//						.keyBy(value -> 1L)
//						.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
//						.reduce(
//								new ReduceFunction<AverageClass>() {
//									@Override
//									public AverageClass reduce(AverageClass a, AverageClass b) {
//											numbercount[0] +=1;
//											return new AverageClass(1L, numbercount[0],(a.sum + b.sum));
//									}
//								});



		//calculates double at every 2 values
		DataStream<AverageClassTwoValue> windowCounts =
				inp.flatMap(
						new FlatMapFunction<String, AverageClassTwoValue>() {
							@Override
							public void flatMap(
									String value, Collector<AverageClassTwoValue> out) {
								for (String word : value.split("\\s")) {
										try {
											Double d = Double.valueOf(word);
											out.collect(new AverageClassTwoValue(1L, d));
										}
										catch (NumberFormatException e){
											System.out.println("Enter valid number: "+e.getMessage());
										}

								}
							}
						})
						.keyBy(value -> 1L)
						.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
						.reduce(
								new ReduceFunction<AverageClassTwoValue>() {
									@Override
									public AverageClassTwoValue reduce(AverageClassTwoValue a, AverageClassTwoValue b) {
										return new AverageClassTwoValue(1L,(a.sum + b.sum)/2);
									}
								});



		windowCounts.print().setParallelism(1);
		System.out.println("Starting on "+hostname+" port: "+port+"\n");
		env.execute("Socket Window WordCount");

	}

}

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

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
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

@SuppressWarnings("serial")
public class StreamingJob {

	/** Data type for words with count. */


	public static class WordWithCount {

		public String word;
		public double avg;
		public WordWithCount() {}
		public WordWithCount(double avg){
			this.avg=avg;
		}

		public double GetAvg(String num){
			long sum=0;
			long cnt=0;
			double avg;
			for (String number : num.split("\\s")) {
				sum+=Integer.valueOf(number);
				cnt+=1;
			}
			avg=(float)sum/(float)cnt;
			return avg;
		}

		@Override
		public String toString() {
			return word + " : " + avg;
		}
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
//			long cpInterval = params.getLong("checkpoint", TimeUnit.MINUTES.toMillis(1));
//			if (cpInterval > 0) {
//				CheckpointConfig checkpointConf = env.getCheckpointConfig();
//				checkpointConf.setCheckpointInterval(cpInterval);
//				checkpointConf.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//				checkpointConf.setCheckpointTimeout(TimeUnit.HOURS.toMillis(1));
//				checkpointConf.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//				env.getConfig().setUseSnapshotCompression(true);
//
//			}


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


		// get input data by connecting to the socket
		DataStream<String> text = env.socketTextStream(hostname, port, "\n");

		// parse the data, group it, window it, and aggregate the counts
		DataStream<WordWithCount> windowCounts =
				text.flatMap(
						new FlatMapFunction<String, WordWithCount>() {
							@Override
							public void flatMap(
									String value, Collector<WordWithCount> out) {
//								for (String word : value.split("\\s")) {
//									out.collect(new WordWithCount(word, 1L));
//								}
//
								WordWithCount a=new WordWithCount();
								double val=0.0;
								val=a.GetAvg(value);
								System.out.println((double)Math.round(val * 100000d) / 100000d);
								WordWithCount b=new WordWithCount(val);
								out.collect(b);
							}
						});
//						.keyBy(value -> value.word)
//						.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
//						.reduce(
//								new ReduceFunction<WordWithCount>() {
//									@Override
//									public WordWithCount reduce(WordWithCount a, WordWithCount b) {
//										return new WordWithCount(a.word, (Integer.valueOf(a.word)+Integer.valueOf(b.word))/2);
//									}
//								});

		// print the results with a single thread, rather than in parallel
		windowCounts.print().setParallelism(1);
		System.out.println("Starting on "+hostname+" port: "+String.valueOf(port)+"\n");
		env.execute("Socket Window WordCount");

	}

}

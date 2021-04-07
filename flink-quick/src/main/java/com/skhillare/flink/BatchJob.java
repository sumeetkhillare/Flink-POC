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

package com.skhillare.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * Skeleton for a Flink Batch Job.
 *
 * <p>For a tutorial how to write a Flink batch application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution,
 * change the main class in the POM.xml file to this class (simply search for 'mainClass')
 * and run 'mvn clean package' on the command line.
 */
@SuppressWarnings("serial")
public class BatchJob {

	public static class WordWithCount {

		public String word;
		public long count;

		public WordWithCount() {}

		public WordWithCount(String word, long count) {
			this.word = word;
			this.count = count;
		}

		@Override
		public String toString() {
			return word + " : " + count;
		}
	}

	public static void main(String[] args) throws Exception {
//		final String file;
		final String file="C:\\Users\\skhillare\\Desktop\\Flink-POC\\flink-batch.txt";
		// get the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final ParameterTool params = ParameterTool.fromArgs(args);
//		try{
//			final ParameterTool params = ParameterTool.fromArgs(args);
//			file = params.get("file");
//
//		}catch(Exception e){
//			System.out.println("please give file");
//		}
//		final ParameterTool params = ParameterTool.fromArgs(args);
//		file = params.get("file");

		DataStream<String> text = env.readTextFile(file);

		// parse the data, group it, window it, and aggregate the counts
		DataStream<WordWithCount> windowCounts =
				text.flatMap(
						new FlatMapFunction<String, WordWithCount>() {
							@Override
							public void flatMap(
									String value, Collector<WordWithCount> out) {
								for (String word : value.split("\\s")) {
									out.collect(new WordWithCount(word, 1L));
								}
							}
						})
						.keyBy(value -> value.word)
						.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
						.reduce(
								new ReduceFunction<WordWithCount>() {
									@Override
									public WordWithCount reduce(WordWithCount a, WordWithCount b) {
										return new WordWithCount(a.word, a.count + b.count);
									}
								});

		// print the results with a single thread, rather than in parallel
		windowCounts.print().setParallelism(1);
		System.out.println(windowCounts);
		System.out.println("Executing env!");
		System.out.println(text);

		env.execute("Socket Window WordCount");


	}


}

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

package com.skhillare.flink_avg;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

@SuppressWarnings("serial")
public class StreamingJob extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>  {
private transient ValueState<Tuple2<Long, Long>> sum;


	//	String input, Collector<Integer> out
	@Override
	public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Long>> out) throws Exception {
		// access the state value
		Tuple2<Long, Long> currentSum = sum.value();
		currentSum.f0 += 1;
		currentSum.f1 += input.f1;
		sum.update(currentSum);
		System.out.println("Current Sum: "+String.valueOf(sum.value().f1)+"\nCurrent Count: "+String.valueOf(sum.value().f0));
		out.collect(new Tuple2<>(input.f0, sum.value().f1 / sum.value().f0));
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



	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final String hostname;
		final Integer port;
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
							+ "--hostname <hostname> --port <port>', where hostname (localhost by default) "
							+ "and port is the address of the text server");
			System.err.println(
					"To start a simple text server, run 'netcat -l <port>' and "
							+ "type the input text into the command line");
			return;
		}

		DataStreamSource<String> inp = env.socketTextStream(hostname, port, "\n");

		DataStream<Tuple2<Long,Long>> windowcounts=inp.flatMap(new FlatMapFunction<String, Tuple2<Long, Long>>() {
			@Override
			public void flatMap(String inpstr, Collector<Tuple2<Long, Long>> out) throws Exception {
				for (String word : inpstr.split("\\s")) {
					try {
						out.collect(Tuple2.of(1L,Long.valueOf(word)));
						}
					catch ( NumberFormatException e) {
						System.out.println("Enter valid number: "+e.getMessage());
						}
				}
			}
			}).keyBy(0).flatMap(new StreamingJob());
			windowcounts.print().setParallelism(1);
			env.execute("Running!");
			return;
		}

}








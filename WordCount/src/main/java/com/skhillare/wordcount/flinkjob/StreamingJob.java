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

package com.skhillare.wordcount.flinkjob;
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

//./bin/flink run -c com.qualys.seca.flink.poc.flinkjob.StreamingJob ~/flink-queryable-1.0-SNAPSHOT.jar --port 9000 --checkpoint 5000

import com.skhillare.wordcount.flinkjob.checkpointing.CustomCheckpointing;
import com.skhillare.wordcount.flinkjob.quitjob.QuitJob;
import com.skhillare.wordcount.initialization.userinput.UserInput;
import com.skhillare.wordcount.manageproperties.ManageFixedProperties;
import com.skhillare.wordcount.manageproperties.ManageFlinkJob;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class StreamingJob{
	public static void main(String[] args) throws Exception {
		final String hostname;
		final Integer port;
		final Integer parallelism;
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		//Get User Input
		ManageFlinkJob properties = UserInput.FlinkJobInput(args);
		ManageFixedProperties fixedProperties = new ManageFixedProperties();
		hostname=properties.getHostname();
		port= properties.getPort();
		parallelism=properties.getParallelism();
		long cpInterval = properties.getCpInterval();
		System.out.println("Hostname: "+hostname+" Port: "+port+" Checkpoint Interval: "+cpInterval+ " Parallelism "+String.valueOf(parallelism));
		CheckpointConfig checkpointConf =CustomCheckpointing.setCheckpointing(env.getCheckpointConfig(),cpInterval);
		env.getConfig().setUseSnapshotCompression(true);
		env.setParallelism(parallelism);
		DataStreamSource<String> inp = env.socketTextStream(hostname, port, "\n");

		inp.flatMap(new FlatMapFunction<String, Tuple2<String ,Long>>() {
			@Override
			public void flatMap(String inpstr, Collector<Tuple2<String,Long>> out) throws Exception{
				//Split numbers from string
				for (String word : inpstr.split("\\s")) {
					try {
						//Quit
						if(word.equals("quit")){
							throw new QuitJob( "Stoppping!!!",hostname,port);
						}
						out.collect(Tuple2.of(word,1L));
					}
					catch (QuitJob ex){
						System.out.println("Quitting!!!");
					}
				}
			}
		}).name("Flatmap for input conversion").keyBy(0).flatMap(new ManageState()).name("Flatmap to store in state").keyBy(0);

		System.out.println("Check Task Slots For Parallelism\nEnter 'quit' to close the server on listening server.\nWordCount Job Running!!!");
		//Execute Job
		env.execute("WordCount Job");
	}
}
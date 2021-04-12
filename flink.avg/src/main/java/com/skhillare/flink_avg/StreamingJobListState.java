
//--port 9000 --checkpoint 5000
//-c com.skhillare.flink_avg.StreamingJobListState

package com.skhillare.flink_avg;

import org.apache.flink.api.common.functions.FlatMapFunction;
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
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

class Quit extends Exception{
    Quit(String m1,String inetAddress,int port) throws IOException {
        super(m1);
        Socket socket = new Socket();
        SocketAddress socketAddress=new InetSocketAddress(inetAddress, port);
        socket.bind(socketAddress);
        socket.close();

    }
}

public class StreamingJobListState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>>{
    private ListState<Tuple2<Long, Long>> listState;

    private void printstate() throws Exception{
        ArrayList<Tuple2<Long, Long>> listdata = Lists.newArrayList(listState.get());
        System.out.println("liststate data");
        for (Tuple2<Long, Long> data : listdata) {
            System.out.print(data.f1.toString()+" ");
            }
        System.out.print("\n");

    }
    @Override
    public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Double>> out) throws Exception {
        Iterable<Tuple2<Long, Long>> state = listState.get();
        if (null == state) {
            listState.addAll(Collections.EMPTY_LIST);
        }
        if (input.f1==-1){
        listState.clear();
        return;
        }
        listState.add(input);
        ArrayList<Tuple2<Long, Long>> listdata = Lists.newArrayList(listState.get());
        if (listdata.size() >= 2) {
            Long count = 0L;
            Long sum = 0L;
            for (Tuple2<Long, Long> data : listdata) {
                count ++;
                sum += data.f1;
            }

            Double res = Double.valueOf(sum) / Double.valueOf(count);
            out.collect(Tuple2.of(input.f0, res));

//            for two number avg
//            listState.clear();
        }
        printstate();
    }

    /** Data type for words with count. */
    @Override
    public void open(Configuration parameters) throws Exception {

        ListStateDescriptor<Tuple2<Long, Long>> listDis = new ListStateDescriptor<>("avg", TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
        }));

        listState = getRuntimeContext().getListState(listDis);
    }

    public static void main(String[] args) throws Exception {
        // the host and the port to connect to
        final String hostname;
        final int port;
        boolean isclear=false;
        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
//			hostname="localhost";
//			port=9000;
            hostname = params.has("hostname") ? params.get("hostname") : "localhost";
            port = params.getInt("port");
//            Configure checkpointing if interval is set
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
        DataStream<String> inp = env.socketTextStream(hostname, port, "\n");

        DataStream<Tuple2<Long,Double>> ListStateAvgRes=inp.flatMap(new FlatMapFunction<String, Tuple2<Long, Long>>() {
            @Override
            public void flatMap(String inpstr, Collector<Tuple2<Long, Long>> out) throws Exception{

                for (String word : inpstr.split("\\s")) {
                    try {
                        if(word.equals("quit")){
                            throw new Quit( "Stoppping!!!",hostname,port);
                        }
                        if(word.equals("clear")){
                            word="-1";
                        }
                        out.collect(Tuple2.of(1L, Long.valueOf(word)));
                    }
                    catch ( NumberFormatException e) {
                        System.out.println("Enter valid number: "+e.getMessage());
                    }catch (Quit ex){
                        System.out.println("Quitting!!!");
                    }
                }
            }
        }).keyBy(0).flatMap(new StreamingJobListState());

        // print the results with a single thread, rather than in parallel
        ListStateAvgRes.print().setParallelism(1);
        System.out.println("Starting on "+hostname+" port: "+(port)+"\n");
        System.out.println("Enter quit to quit and clear to clear the state"+"\n");
        env.execute("Average with ListState");
    }
}


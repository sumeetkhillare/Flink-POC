package com.qualys.seca.flink.poc;
import java.io.PrintWriter;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import jline.console.ConsoleReader;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.queryablestate.client.QueryableStateClient;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
//java -cp ./target/flink-quick-1.0-SNAPSHOT.jar com.qualys.seca.flink.poc.QueryClient -jobid <jobid> -port 9069 -hostname 127.0.1.1
public class QueryClient {

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            throw new IllegalArgumentException("Missing required job ID argument. "
                    + "Usage: ./EventCountClient <jobID>");
        }

        // configuration
        final ParameterTool params = ParameterTool.fromArgs(args);
        String jobIdParam = params.get("jobid");
        final JobID jobId = JobID.fromHexString(jobIdParam);
        final String jobManagerHost =  params.has("hostname") ? params.get("hostname") : "127.0.1.1";
        final int jobManagerPort =  params.has("port") ? Integer.parseInt(params.get("port")) :9069;

        //Creating QueryableStateClient
        QueryableStateClient client = new QueryableStateClient(jobManagerHost, jobManagerPort);
        client.setExecutionConfig(new ExecutionConfig());

        //Description of all states to be queried.
        ValueStateDescriptor<Tuple2<String, Long>> descriptor =
                new ValueStateDescriptor<>(
                        StreamingJob.Value_Q_NAME, // the state name
                        TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                        }), // type information
                        Tuple2.of("avg", 0L));

        ListStateDescriptor<Tuple2<String, Long>> listDis =
                new ListStateDescriptor<>(
                        StreamingJob.List_Value_Q_NAME,
                        TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                        }));

        MapStateDescriptor<String, Long> mapStateDes = new MapStateDescriptor<>(
                StreamingJob.Map_Value_Q_NAME,
                String.class,
                Long.class);


        ReducingStateDescriptor<Tuple2<String,Long>> reducingStateDescriptor= new ReducingStateDescriptor<>(StreamingJob.Reduce_Value_Q_NAME,new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> stringLongTuple2, Tuple2<String, Long> t1){
                return Tuple2.of(stringLongTuple2.f0,stringLongTuple2.f1+t1.f1);
            }
        },TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}));

        System.out.println("Using JobManager " + jobManagerHost + ":" + jobManagerPort);
        printUsage();

        //ConsoleReader for taking input
        ConsoleReader reader = new ConsoleReader();
        reader.setPrompt("\n$ ");
        PrintWriter out = new PrintWriter(reader.getOutput());
        String line;
        while ((line = reader.readLine()) != null) {
            String key = line.toLowerCase().trim();
            out.printf("[info] Querying key '%s'\n", key);
            if(key.equals("statekey")){
                //ValueState Query
                try {
                    long start = System.currentTimeMillis();
                    CompletableFuture<ValueState<Tuple2<String, Long>>> resultFuture =
                            client.getKvState(jobId, StreamingJob.Value_Q_NAME, key, BasicTypeInfo.STRING_TYPE_INFO, descriptor);
                    resultFuture.thenAccept(response -> {
                        try {
                            Tuple2<String, Long> res = response.value();
                            long end = System.currentTimeMillis();
                            long duration = Math.max(0, end - start);
                            if (res != null) {
                                out.printf("\nValueState:\n%s (query took %d ms)\n", res.toString(), duration);
                            } else {
                                out.printf("\nValueState:\nUnknown key %s (query took %d ms)\n", key, duration);
                            }
                        } catch (Exception e) {
                            out.printf("\nNo result for ValueState");
//                            e.printStackTrace();
                        }
                    });
                    resultFuture.get(5, TimeUnit.SECONDS);
                }catch (Exception e) {
                    out.printf("\nUnable to query for ValueState");
//                    e.printStackTrace();
                }
                //ListState Query
                try {
                    long start = System.currentTimeMillis();
                    CompletableFuture<ListState<Tuple2<String, Long>>> resultFutureList =
                            client.getKvState(jobId, StreamingJob.List_Value_Q_NAME, key, BasicTypeInfo.STRING_TYPE_INFO, listDis);
                    resultFutureList.thenAccept(response -> {
                        try {
                            //                            Tuple2<String, Long> res = (Tuple2<String, Long>) response.get();
                            long end = System.currentTimeMillis();
                            long duration = Math.max(0, end - start);
                            Iterable<Tuple2<String, Long>> state = response.get();

                            if (state != null) {
                                out.printf("\nListState:\n%s (query took %d ms)\n", response.get().toString(), duration);
                            } else {
                                out.printf("\nListState:\nUnknown key %s (query took %d ms)\n", key, duration);
                            }
                        } catch (Exception e) {
                            out.printf("\nNo result for ListState");
//                                e.printStackTrace();
                        }
                    });
                    resultFutureList.get(5, TimeUnit.SECONDS);
                }catch (Exception e) {
                    out.printf("\nUnable to query for ListState");
//                    e.printStackTrace();
                }
                //MapState Query
                try {
                    long start = System.currentTimeMillis();
                    CompletableFuture<MapState<String, Long>> resultFutureMap =
                            client.getKvState(jobId, StreamingJob.Map_Value_Q_NAME, key, BasicTypeInfo.STRING_TYPE_INFO, mapStateDes);

                    resultFutureMap.thenAccept(response -> {
                        try {
                            Long res = response.get(StreamingJob.statekey);
                            long end = System.currentTimeMillis();
                            long duration = Math.max(0, end - start);
                            if (res != null) {
                                out.printf("\nMapState:\n%s (query took %d ms)\n", res.toString(), duration);
                            } else {
                                out.printf("\nMapState:\nUnknown key %s (query took %d ms)\n", key, duration);
                            }
                        } catch (Exception e) {
                            out.printf("\nNo result for MapState");
//                            e.printStackTrace();
                        }
                    });
                    resultFutureMap.get(5, TimeUnit.SECONDS);
                }catch (Exception e) {
                    out.printf("\nUnable to query for MapState");
//                    e.printStackTrace();
                }
                //ReducingState Query
                try{
                    long start = System.currentTimeMillis();
                    CompletableFuture<ReducingState<Tuple2<String,Long>>> resultFutureReduce =
                            client.getKvState(jobId, StreamingJob.Reduce_Value_Q_NAME, key, BasicTypeInfo.STRING_TYPE_INFO, reducingStateDescriptor);

                    resultFutureReduce.thenAccept(response -> {
                        try {
                            Tuple2<String, Long> res = response.get();
                            long end = System.currentTimeMillis();
                            long duration = Math.max(0, end - start);
                            if (res != null) {
                                out.printf("\nReducingState:\n%s (query took %d ms)\n", res.f1.toString(), duration);
                            } else {
                                out.printf("\nReducingState:\nUnknown key %s (query took %d ms)\n", key, duration);
                            }
                        } catch (Exception e) {
                            out.printf("\nNo result for ReducingState");
//                            e.printStackTrace();
                        }
                    });

                    resultFutureReduce.get(5,TimeUnit.SECONDS);
                }catch (Exception e) {
                    out.printf("\nUnable to query for ReducingState");
//                    e.printStackTrace();
                }
                //Flatmap opertaor query
                try{
                    long start = System.currentTimeMillis();
                    CompletableFuture<ValueState<Tuple2<String, Long>>> resultFutureKeyed =
                            client.getKvState(jobId, StreamingJob.Keyed_Value_Q_NAME, key, BasicTypeInfo.STRING_TYPE_INFO, descriptor);

                    resultFutureKeyed.thenAccept(response -> {
                        try {
                            Tuple2<String, Long> res = response.value();
                            long end = System.currentTimeMillis();
                            long duration = Math.max(0, end - start);
                            if (res != null) {
                                out.printf("\nKeyed on flatmap operator:\n%s (query took %d ms)\n", res.toString(), duration);
                            } else {
                                out.printf("\nKeyed on flatmap operator:\nUnknown key %s (query took %d ms)\n", key, duration);
                            }
                        } catch (Exception e) {
                            out.printf("\nNo result for keyed on flatmap operator");
//                            e.printStackTrace();
                        }
                    });
                    resultFutureKeyed.get(5,TimeUnit.SECONDS);
                }catch (Exception e) {
                    out.printf("\nUnable to query for keyed on flatmap operator");
//                    e.printStackTrace();
                }
            }else{
                out.printf("You Entered Wrong key, Please enter `statekey` as key.\n");
            }

        }

    }
    private static void printUsage() {
        System.out.println("Enter a key to query.");
        System.out.println();
        System.out.println("The StreamingJob " + StreamingJob.Value_Q_NAME + " state instance "
                + "has key `statekey`.");
        System.out.println();
    }

}

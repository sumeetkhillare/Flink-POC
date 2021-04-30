package com.skhillare.flink;
import java.io.PrintWriter;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import jline.console.ConsoleReader;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.QueryableStateClient;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
//com.skhillare.flink_avg.QueryClass
//java -cp ./target/flink-quick-1.0-SNAPSHOT.jar com.skhillare.flink.QueryClass

public class QueryClient {

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            throw new IllegalArgumentException("Missing required job ID argument. "
                    + "Usage: ./EventCountClient <jobID>");
        }
        String jobIdParam = args[0];

        // configuration
        final JobID jobId = JobID.fromHexString(jobIdParam);
        final String jobManagerHost = "127.0.1.1";
        final int jobManagerPort = 9069;

        QueryableStateClient client = new QueryableStateClient(jobManagerHost, jobManagerPort);
        client.setExecutionConfig(new ExecutionConfig());

        ValueStateDescriptor<Tuple2<String, Long>> descriptor =
                new ValueStateDescriptor<>(
                        StreamingJob.Q_NAME, // the state name
                        TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                        }), // type information
                        Tuple2.of("avg", 0L));

        ListStateDescriptor<Tuple2<String, Long>> listDis =
                new ListStateDescriptor<>(
                        StreamingJob.List_Q_NAME,
                        TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                        }));

        MapStateDescriptor<String, Long> mapStateDes = new MapStateDescriptor<>(
                StreamingJob.Map_Q_NAME,
                String.class,
                Long.class);

        System.out.println("Using JobManager " + jobManagerHost + ":" + jobManagerPort);
        printUsage();

        ConsoleReader reader = new ConsoleReader();
        reader.setPrompt("$ ");
        PrintWriter out = new PrintWriter(reader.getOutput());

        String line;
        while ((line = reader.readLine()) != null) {
            String key = line.toLowerCase().trim();
            out.printf("[info] Querying key '%s'\n", key);
            if(key.equals("statekey")){
                try {
                    long start = System.currentTimeMillis();

                    CompletableFuture<ValueState<Tuple2<String, Long>>> resultFuture =
                            client.getKvState(jobId, StreamingJob.Q_NAME, key, BasicTypeInfo.STRING_TYPE_INFO, descriptor);
                    CompletableFuture<ListState<Tuple2<String, Long>>> resultFutureList =
                                client.getKvState(jobId, StreamingJob.List_Q_NAME, key, BasicTypeInfo.STRING_TYPE_INFO, listDis);
                    CompletableFuture<MapState<String, Long>> resultFutureMap =
                            client.getKvState(jobId, StreamingJob.Map_Q_NAME, key, BasicTypeInfo.STRING_TYPE_INFO, mapStateDes);

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
                            e.printStackTrace();
                        }
                    });
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
                                    e.printStackTrace();
                                }
                    });
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
                            e.printStackTrace();
                        }
                    });
                    resultFutureList.get(5, TimeUnit.SECONDS);
                    resultFuture.get(5, TimeUnit.SECONDS);
                    resultFutureMap.get(5,TimeUnit.SECONDS);

                    } catch (Exception e) {
                        out.printf("Error Processing!!!\n");
                        e.printStackTrace();
                    }
            }else{
                out.printf("You Entered Wrong key, Please enter `statekey` as key.\n");
            }

        }

    }
    private static void printUsage() {
        System.out.println("Enter a key to query.");
        System.out.println();
        System.out.println("The StreamingJob " + StreamingJob.Q_NAME + " state instance "
                + "has key `query`.");
        System.out.println();
    }

}

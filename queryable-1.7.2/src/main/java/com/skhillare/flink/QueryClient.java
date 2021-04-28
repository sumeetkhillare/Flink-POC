package com.skhillare.flink;
import java.io.PrintWriter;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import jline.console.ConsoleReader;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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

        System.out.println("Using JobManager " + jobManagerHost + ":" + jobManagerPort);
        printUsage();

        ConsoleReader reader = new ConsoleReader();
        reader.setPrompt("$ ");
        PrintWriter out = new PrintWriter(reader.getOutput());

        String line;
        while ((line = reader.readLine()) != null) {
            String key = line.toLowerCase().trim();
            out.printf("[info] Querying key '%s'\n", key);

            try {
                long start = System.currentTimeMillis();

                CompletableFuture<ValueState<Tuple2<String, Long>>> resultFuture =
                        client.getKvState(jobId, StreamingJob.Q_NAME, key, BasicTypeInfo.STRING_TYPE_INFO, descriptor);

                resultFuture.thenAccept(response -> {
                    try {
                        Tuple2<String, Long> res = response.value();
                        long end = System.currentTimeMillis();
                        long duration = Math.max(0, end - start);
                        if (res != null) {
                            out.printf("%s (query took %d ms)\n", res.toString(), duration);
                        } else {
                            out.printf("Unknown key %s (query took %d ms)\n", key, duration);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });

                resultFuture.get(5, TimeUnit.SECONDS);

            } catch (Exception e) {
                System.out.println("Unknown Key or Job is failed...\nEnter `query` as key.");
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

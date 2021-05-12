package com.skhillare.wordcount.queryclient;
import java.io.PrintWriter;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.skhillare.wordcount.initialization.userinput.UserInput;
import com.skhillare.wordcount.manageproperties.ManageFixedProperties;
import com.skhillare.wordcount.initialization.statedescriptor.CustomStateDescriptor;
import com.skhillare.wordcount.manageproperties.ManageQueryJob;
import jline.console.ConsoleReader;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.QueryableStateClient;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;


//java -cp ./target/flink-queryable-1.0-SNAPSHOT.jar com.qualys.seca.flink.poc.queryclient.QueryClient -jobid <jobid> -port 9069 -hostname 127.0.1.1
public class QueryClient {
    public static ManageFixedProperties prop = new ManageFixedProperties();

    public static void main(String[] args) throws Exception {
        //User Input
        ManageQueryJob manageQueryJob = UserInput.QueryInput(args);
        final JobID jobId = JobID.fromHexString(manageQueryJob.getJobId());
        final String jobManagerHost = manageQueryJob.getQuery_hostname();
        final int jobManagerPort = manageQueryJob.getQuery_port();
        //Creating QueryableStateClient
        QueryableStateClient client = new QueryableStateClient(jobManagerHost, jobManagerPort);
        client.setExecutionConfig(new ExecutionConfig());
        //Description of all states to be queried.
        ValueStateDescriptor<Tuple2<String ,Long>> descriptor = CustomStateDescriptor.getValueStateDescriptor();
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
                try {
                    long start = System.currentTimeMillis();
                    CompletableFuture<ValueState<Tuple2<String ,Long>>> resultFuture =
                            client.getKvState(jobId, prop.getValue_Q_NAME(), key, BasicTypeInfo.STRING_TYPE_INFO, descriptor);
                    resultFuture.thenAccept(response -> {
                        try {
                            Tuple2<String,Long> res = response.value();
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
        }

    }
    private static void printUsage() {
        System.out.println("Enter a key to query.");
        System.out.println();
        System.out.println("The StreamingJob" + " state instance "
                + "has key word given as input on server.");
        System.out.println();
    }

}

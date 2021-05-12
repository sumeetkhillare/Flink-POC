package com.skhillare.sumjob.queryclient;
import java.io.PrintWriter;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.skhillare.sumjob.initialization.userinput.UserInput;
import com.skhillare.sumjob.manageproperties.ManageFixedProperties;
import com.skhillare.sumjob.initialization.statedescriptor.CustomStateDescriptor;
import com.skhillare.sumjob.manageproperties.ManageQueryJob;
import jline.console.ConsoleReader;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.QueryableStateClient;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;


//java -cp ./target/flink-queryable-1.0-SNAPSHOT.jar com.skhillare.sumjob.queryclient.QueryClient -jobid <jobid> -port 9069 -hostname 127.0.1.1
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
        ValueStateDescriptor<Long> descriptor = CustomStateDescriptor.getValueStateDescriptor();
        ValueStateDescriptor<Tuple2<String, Long>> operatordescriptor = CustomStateDescriptor.getValueStateTupleDescriptor();
        ListStateDescriptor<Long> listDis = CustomStateDescriptor.getListStateDescriptor();
        MapStateDescriptor<String, Long> mapStateDes = CustomStateDescriptor.getMapStateDescriptor();
        ReducingStateDescriptor<Long> reducingStateDescriptor = CustomStateDescriptor.getReducingStateDescriptor();
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
                    CompletableFuture<ValueState<Long>> resultFuture =
                            client.getKvState(jobId, prop.getValue_Q_NAME(), key, BasicTypeInfo.STRING_TYPE_INFO, descriptor);
                    resultFuture.thenAccept(response -> {
                        try {
                            Long res = response.value();
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
                    CompletableFuture<ListState<Long>> resultFutureList =
                            client.getKvState(jobId, prop.getList_Value_Q_NAME(), key, BasicTypeInfo.STRING_TYPE_INFO, listDis);
                    resultFutureList.thenAccept(response -> {
                        try {
                            long end = System.currentTimeMillis();
                            long duration = Math.max(0, end - start);
                            Iterable<Long> state = response.get();

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
                            client.getKvState(jobId, prop.getMap_Value_Q_NAME(), key, BasicTypeInfo.STRING_TYPE_INFO, mapStateDes);

                    resultFutureMap.thenAccept(response -> {
                        try {
                            Long res = response.get(key);
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
                    CompletableFuture<ReducingState<Long>> resultFutureReduce =
                            client.getKvState(jobId, prop.getReduce_Value_Q_NAME(), key, BasicTypeInfo.STRING_TYPE_INFO, reducingStateDescriptor);
                    resultFutureReduce.thenAccept(response -> {
                        try {
                            Long res = response.get();
                            long end = System.currentTimeMillis();
                            long duration = Math.max(0, end - start);
                            if (res != null) {
                                out.printf("\nReducingState:\n%s (query took %d ms)\n", res.toString(), duration);
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
                    CompletableFuture<ValueState<Tuple2<String,Long>>> resultFutureKeyed =
                            client.getKvState(jobId, prop.getKeyed_Value_Q_NAME(), key, BasicTypeInfo.STRING_TYPE_INFO, operatordescriptor);
                    resultFutureKeyed.thenAccept(response -> {
                        try {
                            Tuple2<String,Long> res = response.value();
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
            System.out.println("The StreamingJob" + " state instance "
                    + "has key `statekey`.");
            System.out.println();
        }
}
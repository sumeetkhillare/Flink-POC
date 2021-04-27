package com.skhillare.flink_avg;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.queryablestate.client.QueryableStateClient;
import scala.concurrent.Await;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.Key;
import java.sql.Time;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

//com.skhillare.flink_avg.QueryClass
//java -cp ./target/flink.avg-1.0-SNAPSHOT.jar com.skhillare.flink_avg.QueryClass
public class QueryClass{


    public static void main(String[] args) throws IOException, InterruptedException, Exception {
        QueryableStateClient client = new QueryableStateClient("127.0.1.1", 9069);

//        System.out.println(client);
        System.out.println("Querying on "+args[0]);
        JobID jobId = JobID.fromHexString(args[0]);

        ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
                new ValueStateDescriptor<>(
                        "average",
                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}));


        long k=1L;

        CompletableFuture<ValueState<Tuple2<Long, Long>>> resultFuture =
                client.getKvState(jobId, "query-name","avg", BasicTypeInfo.STRING_TYPE_INFO, descriptor);

        resultFuture.thenAccept(response -> {
            try {
                Tuple2<Long, Long> res = response.value();
                System.out.println("Queried sum value: " + res);
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println("Exiting future ...");
        });
    }

}

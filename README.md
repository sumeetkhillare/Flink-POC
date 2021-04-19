# Flink-POC
Flink Queryable State<br/>
Create server on port 9000 using nc -l 9000<br/>
Go to flink_q_state folder and run ./run-job.sh<br/>
Then copy JobID and run ./run-query-repl.sh (JobID)<br/>
You can input numbers on server and check average by querying avg.<br/>

Use flink-quick-1.0-SNAPSHOT.jar file generated in target folder.<br/>
nc -l 9000<br/>
In flink/bin<br/>
./start-local.sh<br/>
./flink/flink-1.12.2/bin/flink run -c com.skhillare.flink.StreamingJob flink-quick-1.0-SNAPSHOT.jar --port 9000 --checkpoint 2000<br/>

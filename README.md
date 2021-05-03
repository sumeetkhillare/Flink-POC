# Flink-POC
Flink Queryable State<br/>
Create server on port 9000 using nc -l 9000<br/>
Make sure flink 1.7.2 is up -- start using ./bin/start-cluster.sh <br/>
Go to flink 1.7.2 folder and run ./bin/flink run -c com.skhillare.flink.StreamingJob (path to Flink-POC)/queryable-1.7.2/target/flink-quick-1.0-SNAPSHOT.jar --port 9000 --checkpoint 5000<br/>
Then copy JobID from flink web dashboard for 'Sum Job' and run ./run-query-repl.sh (JobID)<br/>
You can input numbers on server running on 9000 port and check sum by querying 'statekey'.<br/>

Use flink-quick-1.0-SNAPSHOT.jar file generated in target folder.<br/>
nc -l 9000<br/>
In flink/bin<br/>
./start-local.sh<br/>
./flink/flink-1.12.2/bin/flink run -c com.skhillare.flink.StreamingJob flink-quick-1.0-SNAPSHOT.jar --port 9000 --checkpoint 2000<br/>

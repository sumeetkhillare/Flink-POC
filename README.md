# Flink-POC
Use flink-quick-1.0-SNAPSHOT.jar file generated in target folder.<br/>
nc -l 9000<br/>
In flink/bin<br/>
./start-local.sh<br/>
./flink/flink-1.12.2/bin/flink run -c com.skhillare.flink.StreamingJob flink-quick-1.0-SNAPSHOT.jar --port 9000 --checkpoint 2000<br/>
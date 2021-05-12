XDR Internship<br/>
Flink Queryable States POC<br/><br/>
Contents in Flink-POC:<br/>
Output Screenshots: Screenshots of running flink job and queryable state.<br/>
SumJob: Code for queryable SumJob for different states.<br/>
WordCount: Code for queryable WordCount job<br/><br/>
Configuration:<br/>
To enable queryable state on your Flink cluster, you just have to copy the flink-queryable-state-runtime_2.11-1.7.2.jar from the opt/ folder of your Flink distribution, to the lib/ folder.<br/><br/>
How to run?<br/>
Start flink-1.7.2 cluster using ./bin/start-cluster.sh<br/>
Start server on 9000 port using 'nc -l 9000'<br/>
Submit flink job to cluster using:<br/>
./run-job.sh (path to bin/flink) (port of running server) (checkpointing time) (parallelism)<br/>
Ex:<br/>
./run-job.sh ~/flinkinstalled/flink-1.7.2/bin/flink 9000 5000<br/>
To query flink state:<br/>
./run-query-repl.sh (jobid)<br/><br/>
For SumJob:<br/>
Use 'statekey' as key<br/>
For WordCount Job:<br/>
Use word as a key<br/><br/>
For Queryable-1.2:<br/>
./run-job.sh<br/>
./run-query-repl.sh (jobid)<br/>
#!/usr/bin/env bash
export TERM=xterm-color
base="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
target="$base/target/flink-quick-1.0-SNAPSHOT.jar"

if [ ! -f $target ]; then
  echo "[info] Building demo JAR... this can take a few seconds"
  mvn clean package -DskipTests &> /dev/null
  echo "[info] Done. Demo JAR created in $target"
  echo ""
fi

if [ -z $1 ]; then
  echo "Usage: run-query-repl.sh <jobId>";
  exit 1
else
  jobId=$1
  shift
  echo "[info] Querying job with ID '$jobId'";
fi

echo "[info] Executing EventCountClient from flink-quick-1.0-SNAPSHOT.jar (exit via Control+C)"
echo ""
java -cp $target com.qualys.seca.flink.poc.QueryClient -jobid $jobId

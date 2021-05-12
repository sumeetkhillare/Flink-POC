#!/usr/bin/env bash

base="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
target="$base/target/wordcount-queryable-1.0-SNAPSHOT.jar"
if [ ! -f $target ]; then
  echo "[info] Building demo JAR... this can take a few seconds"
  mvn clean package -DskipTests &> /dev/null
  echo "[info] Done. Demo JAR created in $target"
  echo ""
fi

if [ -z $1 ] || [ -z $2 ] || [ -z $3 ] || [ -z $4 ]; then
  echo "Usage: run-query-repl.sh <path to bin/flink> <port of running server> <checkpointing time> <parallelism>";
  exit 1
else
  flinkpath=$1
  port=$2
  ctime=$3
  parallelism=$4
  /bin/bash $flinkpath run -c com.skhillare.wordcount.flinkjob.StreamingJob $target --port $port --checkpoint $ctime --parallelism $parallelism
  shift
  echo "[info] Submitting job with path '$flinkpath'";
fi

#!/bin/bash

SPARK_WORKLOAD=$1

echo "SPARK_WORKLOAD: $SPARK_WORKLOAD"

if [ "$SPARK_WORKLOAD" == "master" ];
then
  start-master.sh -p 7077

elif [ "$SPARK_WORKLOAD" == "worker" ];
then

  WORKER_INDEX=$(hostname | grep -o '[0-9]*$')

  WEBUI_PORT=$((8080 + WORKER_INDEX))
  echo "Starting worker on port: $WEBUI_PORT"

  start-worker.sh spark://spark-master:7077 --webui-port $WEBUI_PORT

fi

#!/bin/bash

SPARK_WORKLOAD=$1

echo "SPARK_WORKLOAD: $SPARK_WORKLOAD"

if [ "$SPARK_WORKLOAD" == "master" ]; then

  start-master.sh -p 7077

elif [ "$SPARK_WORKLOAD" == "worker" ]; then

  HOSTNAME=$(hostname)

  WORKER_INDEX=$(echo "$HOSTNAME" | grep -o -E '[0-9]+$')

  if [ -z "$WORKER_INDEX" ]; then

    WEBUI_PORT=8081

  else

    WEBUI_PORT=$((8090 + WORKER_INDEX))

  fi

  start-worker.sh spark://spark-master:7077 --webui-port $WEBUI_PORT

else

  echo "Unknown SPARK_WORKLOAD: $SPARK_WORKLOAD"

  exit 1
  
fi

tail -f /dev/null

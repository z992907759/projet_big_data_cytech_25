#!/bin/bash

if [ "$SPARK_MODE" = "master" ]; then
  /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master \
    --host spark-master \
    --port 7077 \
    --webui-port 8080
elif [ "$SPARK_MODE" = "worker" ]; then
  /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
    ${SPARK_MASTER_URL} \
    --memory ${SPARK_WORKER_MEMORY:-1G} \
    --cores ${SPARK_WORKER_CORES:-1}
else
  echo "Error: SPARK_MODE must be set to 'master' or 'worker'"
  exit 1
fi

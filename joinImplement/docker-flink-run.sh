#!/bin/bash

# JOB_CLASS_NAME="cn.xingzheng.HbaseOnFlink.FlinkHBaseDemo"
JM_CONTAINER=$(docker ps --filter name=jobmanager --format={{.ID}})
# TM_CONTAINER=$(docker ps --filter name=taskmanager --format={{.ID}})
# docker exec -t -i "${JM_CONTAINER}" sh /editHosts.sh
# docker exec -t -i "${TM_CONTAINER}" sh /editHosts.sh
docker cp ./target/Join-1.0-SNAPSHOT.jar "${JM_CONTAINER}":/job.jar
# docker exec -t -i "${JM_CONTAINER}" flink run -d -c ${JOB_CLASS_NAME} /job.jar
docker exec -t -i "${JM_CONTAINER}" flink run -d /job.jar
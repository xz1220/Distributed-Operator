#!/bin/bash

# JOB_CLASS_NAME="cn.xingzheng.HbaseOnFlink.FlinkHBaseDemo"
JM_CONTAINER=$(docker ps --filter name=jobmanager --format={{.ID}})
docker cp ./target/hbase-test-1.0-SNAPSHOT.jar "${JM_CONTAINER}":/job.jar
# docker exec -t -i "${JM_CONTAINER}" flink run -d -c ${JOB_CLASS_NAME} /job.jar
docker exec -t -i "${JM_CONTAINER}" flink run -d /job.jar
# Build flink and Hbase cluster using docker
## flink
following the instructions of Flink Doc: [Docker设置](https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/deployment/resource-providers/standalone/docker.html)

<details>
<summary><strong>Application model</strong></summary>
Under the application model ,you need to specify the path of application package *.jar and the class name when you create the docker containers.
If you don't specify the right path and class name, cluster will fail when started.
docker-compose configuration should be like this:

```shell

command: standalone-job --job-classname WordCount.jar
    - /home/xingzheng/Distributed-Operator/job/batch:/opt/flink/usrlib

```
</details>

**Most of time ,we use session model, cause we need to upload the different \*.Jar several times.**
<details>
<summary><strong>Session model</strong></summary>
Under the session model, we can use multiple ways to start jobs, using web UI or CLI command.
CLI command like this:

```shell

$ JOB_CLASS_NAME="com.job.ClassName"
$ JM_CONTAINER=$(docker ps --filter name=jobmanager --format={{.ID}}))
$ docker cp path/to/jar "${JM_CONTAINER}":/job.jar
$ docker exec -t -i "${JM_CONTAINER}" flink run -d -c ${JOB_CLASS_NAME} /job.jar

```

</details>

## hbase



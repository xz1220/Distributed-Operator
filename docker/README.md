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
fork from the [big-data-europe/docker-hbase](https://github.com/big-data-europe/docker-hbase), and update the hbase version to 2.0.0
<details>
<summary><strong>Build your own Hbase image</strong></summary>

1. enter ./docker/storage/base
2. edit the dockerfile and entrypoint.sh
3. build your own image

command like this:
```shell

cd ./docker/base
vim < Dockerfile or entrypoint>
docker build -t <your image name:version> .

```

</details>

In the docker-compose-distributed-local.yml, we define a hadooop cluster and Hbase cluster. And we sink hadoop data to local filesystem, such as ./docker/data/*.

run and distory the cluster like this:

```shell

cd <project path>
# run the hbase cluster backend.
docker-compose -f ./docker/storage/docker-compose-distributed-local.yml up -d 
# stop or distroy cluster
docker-compose -f ./docker/storage/docker-compose-distributed-local.yml stop
docker-compose -f ./docker/storage/docker-compose-distributed-local.yml down

```

## combine the flink and hbase

As the restriction of version of flink-hbase maven-package, the version of flink used in the cluster is a little bit from the before. flinkHbase.yml is the configuration of clusters. Version details as following:
- flink: v7.2
- hbase: v2.0.0
- hadoop: v2.7.4

<details>
<summary><strong>some steps before using</strong></summary>
Of course, you need to install docker and docker-compose. and you need to edit the flinkHbase.yml to using you own image. 
Under the path ./computing, editHosts.sh add the <\ip, hosts\> of Hbase-master and Hbase-regionServer to /etc/hosts, so that your flink jobs can access the hbase cluster. Howerver, when you create the flink container, the image will recreate the /etc/hosts. 
So one of solutions is to run this script in the container. It is also noticiable that flink cluster will create a specfic network, differing from the docker network, unless you allocate a ip_address to the container in the config file.
Like you see in the flinkHbase.yml:

```yml
  jobmanager:
    image: danxing/flink:latest
    expose:
      - "6123"
    ports:
      - "8081:8081"
    command: jobmanager
    environment:
      - JOB_MANAGER_RPC_ADDRESS=jobmanager
    links:
      - zoo:zookeeper
      - hbase-master
      - hbase-region:hbase-regionserver
    networks:
      flinkHbase: 
        ipv4_address: 172.27.0.10   # allocate a ipv4 address

  taskmanager:
    image: danxing/flink:latest
    expose:
      - "6121"
      - "6122"
    depends_on:
      - jobmanager
    command: taskmanager
    links:
      - "jobmanager:jobmanager"
    environment:
      - JOB_MANAGER_RPC_ADDRESS=jobmanager
    links:
      - zoo:zookeeper
      - hbase-master
      - hbase-region:hbase-regionserver
    networks:
      flinkHbase: 
        ipv4_address: 172.27.0.11   # allocate a ipv4 address
```

</details>



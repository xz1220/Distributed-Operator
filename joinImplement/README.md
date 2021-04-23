# Join Implement
This part is to show the examples about how to use jave to manipulate Hbase and how the flink connect to Hbase using java.

<details>
<summary><strong>basic setup</strong></summary>
The code use Java 1.8 and Maven as dependcies management. So you need to install them in you environment.

```shell
mvn install     # install the dependency
mvn package     # package the code
java -jar <path to jar>     # run the code locally
```

</details>

under the src/main/java, the code shows how to do all things. Edit it to suit your needs.

## Hbase Cluster

### Tables infor

#### User

<details>
<summary><strong> summary </strong></summary>

</details>

#### Order

<details>
<summary><strong> summary </strong></summary>

```shell
 HBase Counters
                BYTES_IN_REMOTE_RESULTS=8780120064
                BYTES_IN_RESULTS=8780120064
                MILLIS_BETWEEN_NEXTS=573447
                NOT_SERVING_REGION_EXCEPTION=0
                NUM_SCANNER_RESTARTS=0
                NUM_SCAN_RESULTS_STALE=0
                REGIONS_SCANNED=2
                REMOTE_RPC_CALLS=6823
                REMOTE_RPC_RETRIES=0
                ROWS_FILTERED=6821
                ROWS_SCANNED=162594816      # rows of content
                RPC_CALLS=6823
                RPC_RETRIES=0
```

</details>

#### Case

<details>
<summary><strong> summary </strong></summary>

```shell

                FILE: Number of write operations=0
                HDFS: Number of bytes read=0
                HDFS: Number of bytes written=0
                HDFS: Number of read operations=0
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=0
        Map-Reduce Framework
                Map input records=111065548
                Map output records=0
                Input split bytes=481
                Spilled Records=0
                Failed Shuffles=0
                Merged Map outputs=0
                GC time elapsed (ms)=1062
                Total committed heap usage (bytes)=506855424
        HBase Counters
                BYTES_IN_REMOTE_RESULTS=5775408496
                BYTES_IN_RESULTS=5775408496
                MILLIS_BETWEEN_NEXTS=716350
                NOT_SERVING_REGION_EXCEPTION=0
                NUM_SCANNER_RESTARTS=0
                NUM_SCAN_RESULTS_STALE=0
                REGIONS_SCANNED=2
                REMOTE_RPC_CALLS=4554
                REMOTE_RPC_RETRIES=0
                ROWS_FILTERED=4552
                ROWS_SCANNED=111065548
                RPC_CALLS=4554
                RPC_RETRIES=0
        org.apache.hadoop.hbase.mapreduce.RowCounter$RowCounterMapper$Counters
                ROWS=111065548
        File Input Format Counters 
                Bytes Read=0
        File Output Format Counters 
                Bytes Written=0
```




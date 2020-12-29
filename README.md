# Distributed Operator

## Assumption

### Single Node

- 磁盘IO开销远大于内存计算 
- 尽可能减少磁盘IO

### Distributed Environment

- **<u>网络开销** 和 **磁盘IO** 都是性能的考量因素</u> 
  - 在读取和写入的情况下，网络开销可以和写入开销放在一起。
  - 假设关系为R，存储R的所有元组需要大小为**B**个块，<u>**读取的IO代价为B**</u>
  - 假设传输一个块的网络开销为**t**，那么<u>**传输所有块的网络开销为B*t**</u>
  - 在对于一个关系R进行hash存储之后，假定<u>**存储块的大小会变成L(B)**</u>
  - 对于单个计算节点来说，我们<u>**假设每一个计算节点的内存可以容纳M块Block**</u>
  - **<u>假设有K台计算节点</u>**
- 尽可能减小网络开销，计算逻辑尽可能下放到存储节点（存算不分离）。

## 流式与mini-batch计算框架

### Storm

**<u>docker 部署，组件包含zookeeper，storm。</u>**目前有一个zookeeper节点，一个storm nimbus（主节点: some-nimbus）节点，一个 storm  supervisor（从节点），一个storm ui节点。

<img src="./doc/Image/Storm_architecture.png" alt="Storm_arch" style="zoom: 50%;" />

#### 向集群提交topology

```bash
# 向主节点容器中提交Jar包，并执行
docker run --link some-nimbus:nimbus -it --rm -v $(pwd)/topology.jar:/topology.jar storm storm jar /topology.jar org.apache.storm.starter.WordCountTopology topology
```

- 向主节点some-nimbus提交topology（jar包）并执行

### Spark

### MapReduce



## 存储方案调研

### 使用KV单节点

#### levelDB / RocksDB



## 算子方案实现

**<u>方案划分按照关系的存储大小</u>**，针对于不同的情况选取不同的方案和框架来做。

### 与内存大小无关，天然支持流式处理的算子

| 单目算子 | 分布式方案                                                   | 流式支持                                    | 开销分析 | 存储分析 |
| -------- | ------------------------------------------------------------ | ------------------------------------------- | ---------- | ---------- |
| 选择     | **Storm**: 多个Spout读取数据，通过ShuffleGrouping的方式随机分配给多个Bolt消费，流式输出结果。 | 支持                                        | B+2Bt | 与大小无关 |
| 投影     | **Storm**: 多个Spout读取数据，通过ShuffleGrouping的方式随机分配给多个Bolt消费，流式输出结果。 | 支持 | B+2Bt | 与大小无关 |
| 并 | **storm**：多个spout直接读取关系R和S，用shuffleGrouping随机分配给多个bolt，直接输出 | 支持 | B(R)+B(S)+2B(R)t+2B(S)*t | 与大小无关 |

##### 图示

<img src="./doc/Image/filter&projector.png" alt="filter&projector" style="zoom: 67%;" />

<img src="./doc/Image/Image_03.png" alt="并" style="zoom:33%;" />



### 关系较小，可以基于内存的方案 ：使用框架 storm

- 对于**双目算子**而言：若其中较小的关系可以被完全的用内存兜住，那么**<u>在完成最初的hash之后，也能够支持流式输出</u>**。
- 排序算子：**<u>使用storm框架模拟多路归并</u>**
- （集合&聚集） && （并&差&自然连接）： 直接使用storm进行模拟

**基于storm的问题**：

- 

单目算子

| 单目算子 | 分布式方案                                                   | 流式支持                                    | 开销分析 | 存储分析 |
| :------- | ------------------------------------------------------------ | ------------------------------------------- | ---------- | ---------- |
| **排序** | **Storm**：多个Spout读取数据，通过通过fieldsGrouping的方式按field将具有相同的值的tuple发送给同一个bolt, 单个bolt内部进行排序。这一阶段完成后，最后由一个汇总的bolt进行总的排序，类似于多路归并的第二阶段，通过**Tuple#getSourceComponent**获取源bolt. | 支持                                    | **B+3B*t** | **K*M** |
| **集合** | **Storm**: 多个Spout 读取数据，通过fieldsGrouping的方式按field将具有相同的值的tuple发送给同一个bolt, 单个bolt内部，若内存中无此项，则输出，若内存中有，则什么也不做。 | 支持                                        | **B+2B*t** | **K*M** |
| **聚合** | **Storm**: 多个spout读取数据，通过fieldsGrouping的方式按field将具有相同的值的tuple发送给同一个bolt, 单个bolt内部进行聚合操作,在cleanup阶段输出。**不同的聚合函数在后续处理时有细微的差别。** | 可以使用Storm实现，**但是整体逻辑是批处理** | **B+2B*t** | **K*M** |



<img src="./doc/Image/Sorting.png" alt="filter&projector" style="zoom: 67%;" />

<img src="./doc/Image/SET.png" alt="filter&projector" style="zoom: 67%;" />

<img src="./doc/Image/grouping.png" alt="filter&projector" style="zoom: 67%;" />

#### 双目算子

<u>**假设相比于R来说，S是较小的**</u>

| 双目算子     | 分布式方案                                                   | 流式支持 | 开销分析                         | 存储分析 |
| ------------ | ------------------------------------------------------------ | -------- | -------------------------------- | -------- |
| **自然连接** | **Storm**：  多个spout读取关系S，并用fieldGrouping的方式发往bolt进行hash存储，完成后发往下一阶段的bolt，下一段的bolt接受多个stream，并进行匹配计算，匹配成功则进行连接并输出。 | 支持     | **B(S)+B(R)+B(S)t+L(S)t+2B(R)t** |          |
| **交**       | **storm**：多个spout读取关系S，并用fieldGrouping的方式发往bolt进行hash存储，完成后发往下一阶段的bolt，下一段的bolt接受多个stream，进行s的分片与R全表匹配,匹配成功后输出并hash表计数减一，直至0。 | 支持     | **B(S)+B(R)+B(S)t+L(S)t+2B(R)t** |          |
| **差**       | **storm**：多个spout读取关系S，并用fieldGrouping的方式发往bolt进行hash存储，完成后发往下一阶段的bolt，下一段的bolt接受多个stream，进行s的分片与R全表匹配.匹配成功计数器-1，不成功或者计数器为0时，输出元组。 | 支持     | **B(S)+B(R)+B(S)t+L(S)t+2B(R)t** |          |

##### 图示

<img src="./doc/Image/Image_01.png" alt="naturalJoin" style="zoom: 33%;" />

<img src="./doc/Image/Image_02.png" alt="union" style="zoom:33%;" />

<img src="./doc/Image/Image_04.png" alt="差" style="zoom:33%;" />

### 关系很大, 只转存一次中间结果

#### 散列表（hash）

| 算子     | 分布式方案 | 流式支持 | 开销分析 | 存储分析 |
| -------- | ---------- | -------- | -------- | -------- |
| 去重     |            | 批处理   |          |          |
| 分组     |            |          |          |          |
| 聚合     |            |          |          |          |
| 自然连接 |            |          |          |          |
| 交       |            |          |          |          |
| 差       |            |          |          |          |



#### 多路归并分布式化

- 排序：在表真的很大但是内存真的很受限制的情况下，需要进行多次迭代来进行操作。但是这样效率会非常低，

- 

### 一些问题的集合

##### 假设可用的内存为16GB，但关系的大小为20G。在这种情况下，完全应用关系很大的情况下的方案是否不合适，是否也可以部分利用流式处理框架？


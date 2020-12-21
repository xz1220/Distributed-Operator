# Distributed Operator

## Assumption

### Single Node

- 磁盘IO开销远大于内存计算 
- 尽可能减少磁盘IO

### Distributed Environment

- 网络开销 > 磁盘IO
- 尽可能减小网络开销，计算逻辑尽可能下放到存储节点
- 在优化生成物理执行计划的时候，应该尽量减少 leader 节点的执行算子复杂度

## 流式与mini-batch计算框架

### storm

docker 部署，组件包含zookeeper，storm。目前有一个zookeeper节点，一个storm nimbus（主:some-nimbus）节点，一个 storm  supervisor（从）节点，一个storm ui节点。

<img src="./doc/Image/Storm_architecture.png" alt="Storm_arch" style="zoom: 50%;" />

#### 向集群提交topology

```bash
docker run --link some-nimbus:nimbus -it --rm -v $(pwd)/topology.jar:/topology.jar storm storm jar /topology.jar org.apache.storm.starter.WordCountTopology topology
```

- 向主节点some-nimbus提交topology（jar包）并执行

## 方案汇总

### 基于storm

**基于storm的问题**：

Storm是无状态的，中间计算结果等等都保存在内存中，表的大小会受限制于内存。若计算节点可以无限制扩容。

#### 单目算子

| 单目算子 | 单机方案                                      | 分布式方案                                                   | 流式支持                                    |
| -------- | --------------------------------------------- | ------------------------------------------------------------ | ------------------------------------------- |
| 选择     | 一趟算法，读取多个Batch进入内存，然后进行处理 | **Storm**: 多个Spout读取数据，通过ShuffleGrouping的方式随机分配给多个Bolt消费，流式输出结果。 | 支持                                        |
| 投影     | 一趟算法，读取多个Batch进入内存，然后进行处理 | **Storm**: 多个Spout读取数据，通过ShuffleGrouping的方式随机分配给多个Bolt消费，流式输出结果。 | 支持                                        |
| 排序     |                                               | **Storm**：多个Spout读取数据，通过通过fieldsGrouping的方式按field将具有相同的值的tuple发送给同一个bolt, 单个bolt内部进行去重+排序。这一阶段完成后，最后由一个汇总的bolt进行总的排序，类似于多路归并的第二阶段，通过**Tuple#getSourceComponent**获取源bolt. | 流式？                                      |
| 集合     |                                               | **Storm**: 多个Spout 读取数据，通过fieldsGrouping的方式按field将具有相同的值的tuple发送给同一个bolt, 单个bolt内部，若内存中无此项，则输出，若内存中有，则什么也不做。 | 支持                                        |
| 聚集     |                                               | **Storm**: 多个spout读取数据，通过fieldsGrouping的方式按field将具有相同的值的tuple发送给同一个bolt, 单个bolt内部进行聚合操作。**不同的聚合函数在后续处理时有细微的差别。** | 可以使用Storm实现，**但是整体逻辑是批处理** |

#### 双目算子

| 双目算子 | 单机方案 | 分布式方案                                                   | 流式支持 |
| -------- | -------- | ------------------------------------------------------------ | -------- |
| 自然连接 |          | **Storm**：  多个spout读取关系S，并用fieldGrouping的方式发往bolt进行hash存储，完成后发往下一阶段的bolt，下一段的bolt接受多个stream，并进行匹配计算，匹配成功则进行连接并输出。 | 支持？   |
| 交       |          | **storm**：多个spout读取关系S，并用fieldGrouping的方式发往bolt进行hash存储，完成后发往下一阶段的bolt，下一段的bolt接受多个stream，进行s的分片与R全表匹配,匹配成功后输出并hash表计数减一，直至0。 |          |
| 并       |          | **storm**：多个spout直接读取关系R和S，用shuffleGrouping随机分配给多个bolt，直接输出 |          |
| 差       |          | **storm**：多个spout读取关系S，并用fieldGrouping的方式发往bolt进行hash存储，完成后发往下一阶段的bolt，下一段的bolt接受多个stream，进行s的分片与R全表匹配.匹配成功计数器-1，不成功或者计数器为0时，输出元组。 |          |

<img src="./doc/Image/Image_01.png" alt="naturalJoin" style="zoom: 33%;" />

<img src="./doc/Image/Image_02.png" alt="union" style="zoom:33%;" />

<img src="./doc/Image/Image_03.png" alt="并" style="zoom:33%;" />

<img src="./doc/Image/Image_04.png" alt="差" style="zoom:33%;" />




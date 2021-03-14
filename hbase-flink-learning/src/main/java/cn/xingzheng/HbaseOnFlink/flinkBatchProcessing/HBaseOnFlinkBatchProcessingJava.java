package cn.xingzheng.HbaseOnFlink.flinkBatchProcessing;

import cn.xingzheng.HbaseOnFlink.HBaseInputFormatJava;
import cn.xingzheng.HbaseOnFlink.HBaseOutputFormatJava;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * @Author: Yang JianQiu
 * @Date: 2019/3/15 0:41
 *  flink dataSet 批处理读写HBase
 *  读取HBase数据方式：实现TableInputFormat接口
 *  写入HBase方式：实现OutputFormat接口
 */
public class HBaseOnFlinkBatchProcessingJava {

    /**
     * 读取HBase数据方式：实现TableInputFormat接口
     */
    public void  readFromHBaseWithTableInputFormat() throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<String, String>> dataSet = env.createInput(new HBaseInputFormatJava());

        dataSet.filter(new FilterFunction<Tuple2<String, String>>() {
            @Override
            public boolean filter(Tuple2<String, String> tuple2) throws Exception {
                return tuple2.f1.startsWith("20");
            }
        });

        dataSet.print();
        //读取数据，则可以不用env.execute()，这句代码针对的是data sinks写入数据的
        // env.execute();
    }

    /**
     * 写入HBase方式：实现OutputFormat接口
     */
    public void write2HBaseWithOutputFormat() throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2.定义数据
        DataSet<String> dataSet = env.fromElements("103,zhangsan,20", "104,lisi,21", "105,wangwu,22", "106,zhaolilu,23");
        dataSet.output(new HBaseOutputFormatJava());
        //运行下面这句话，程序才会真正执行，这句代码针对的是data sinks写入数据的
        env.execute();
    }
}

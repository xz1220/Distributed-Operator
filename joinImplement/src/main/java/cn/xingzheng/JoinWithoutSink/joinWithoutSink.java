package cn.xingzheng.JoinWithoutSink;

import cn.xingzheng.HbaseOnFlink.HBaseInputFormatJava;
import cn.xingzheng.HbaseOnFlink.HBaseOutputFormatJava;
import cn.xingzheng.HbaseOnFlink.HBaseReaderJava;
import cn.xingzheng.HbaseOnFlink.HBaseWriterJava;
import org.apache.commons.net.ntp.TimeStamp;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Date;
import java.util.Properties;
import java.util.*;
import cn.xingzheng.DataType.*;
import cn.xingzheng.Utils.*;
import scala.annotation.meta.param;
import scala.xml.PrettyPrinter.Para;

public class joinWithoutSink {
    
    public static void main(String[] args) throws Exception{
        System.out.println("This is test!");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        ArrayList<String> parameter = new ArrayList<String>();
        parameter.add("English");
        parameter.add("Chinese");
        parameter.add("Math");
        ReadingHbase source = new ReadingHbase("gradesV1", parameter);
        DataStream<Grades> dataStream = env.addSource(source);

         dataStream.map(new MapFunction<Grades, Object>() {
             @Override
             public Object map(Grades value) throws Exception {
                 System.out.println(value.studentID + " " + value.ChineseGrade);
                 return null;
             }
         });


        ArrayList<String> parameter2 = new ArrayList<String>();
        parameter2.add("Name");
        ReadingHbase2 source2 = new ReadingHbase2("name", parameter2);
        DataStream<Name> dataStream2 = env.addSource(source2);

       dataStream2.keyBy((Name name) -> name.studentName)
            .map(new MapFunction<Name, Object>() {
                @Override
                public Object map(Name name) throws Exception {
                    System.out.println(name.studentID + " " + name.studentName);
                    return null;
                }
            });

       dataStream2.connect(dataStream)
               .flatMap(new RichCoFlatMapFunction<Name, Grades, Object>() {
                   private ListState<Name> nameStates;
                   private ListState<Grades> gradeStates;

                   @Override
                   public void open()

                   @Override
                   public void flatMap1(Name name, Collector<Object> collector) throws Exception {
                       if
                   }

                   @Override
                   public void flatMap2(Grades grades, Collector<Object> collector) throws Exception {

                   }
               })


        env.execute();
    }
}


/**
 * TODO: 
 *  Q1: 重复使用new 同一个类会导致配置的覆盖，查找为什么？
 *      目前的解决方法是，使用两个类似的类，
 */

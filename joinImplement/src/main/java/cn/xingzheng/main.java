package cn.xingzheng;

import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;

import cn.xingzheng.JoinWithoutSink.joinWithoutSink;
import cn.xingzheng.Utils.*;

import cn.xingzheng.HbaseOnFlink.HBaseInputFormatJava;
import cn.xingzheng.HbaseOnFlink.HBaseOutputFormatJava;
import cn.xingzheng.HbaseOnFlink.HBaseReaderJava;
import cn.xingzheng.HbaseOnFlink.HBaseWriterJava;
import org.apache.commons.net.ntp.TimeStamp;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.awt.*;
import java.security.Key;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.*;
import cn.xingzheng.DataType.*;
import cn.xingzheng.Utils.*;
import org.apache.hbase.thirdparty.com.google.protobuf.MapEntry;
import org.apache.kafka.common.protocol.types.Field;
import org.junit.Rule;
import scala.annotation.meta.param;
import scala.xml.PrettyPrinter.Para;

public class main {
  public static void main(String[] args) throws Exception {
      // CommandLineParser parser = new BasicParser();
      // Options options = new Options();
      // options.addOption("h", "help", false , "Print this usage information");
      // options.addOption("f", "function", true, "utils.hbase for operating the hbase");

      // try {
      //   CommandLine commandLine = parser.parse(options, args);
      //   String function = "";

      //   if (commandLine.hasOption("h")) {
      //       System.out.println("This is help!");
      //   }
  
      //   if (commandLine.hasOption("f")) {
      //         function = commandLine.getOptionValue("f");
      //         if (function.compareTo("utils") == 0) {
      //               HBaseJavaApiDemo.insertCasesForStream();;
      //         }else{
      //             System.out.println("Arguments Error");
      //         }
      //   }

      // }catch (Exception e) {
      //     e.printStackTrace();
      // }
      
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
        KeyedStream<Grades,String> keyedGrades = dataStream.keyBy(Grades::getStudentID);


        ArrayList<String> parameter2 = new ArrayList<String>();
        parameter2.add("Name");
        ReadingHbase2 source2 = new ReadingHbase2("name", parameter2);
        DataStream<Name> dataStream2 = env.addSource(source2);

        MapStateDescriptor<String, Name> ruleMapStateDescriptor = new MapStateDescriptor<>(
                "RulesBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<Name>() {}));
        BroadcastStream<Name> broadCastName = dataStream2.broadcast(ruleMapStateDescriptor);

        DataStream<String> out = keyedGrades
                .connect(broadCastName)
                .process(
                        new KeyedBroadcastProcessFunction<String, Grades, Name, String>() {
                            private MapStateDescriptor<String,List<Grades>> mapStateDescriptor =
                                    new MapStateDescriptor<>(
                                            "grades",
                                            BasicTypeInfo.STRING_TYPE_INFO,
                                            new ListTypeInfo<>(Grades.class)
                                    );
                            private MapStateDescriptor<String, Name> ruleMapStateDescriptor = new MapStateDescriptor<>(
                                    "RulesBroadcastState",
                                    BasicTypeInfo.STRING_TYPE_INFO,
                                    TypeInformation.of(new TypeHint<Name>() {}));


                            @Override
                            public void processBroadcastElement(Name value,
                                                                Context ctx,
                                                                Collector<String> out) throws Exception {
                                // System.out.println("processBroadcastElement:  "+value.toString());
                                ctx.getBroadcastState(ruleMapStateDescriptor).put(value.studentID, value);

                            }

                            @Override
                            public void processElement(Grades value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                                final MapState<String, List<Grades>> state = getRuntimeContext().getMapState(mapStateDescriptor);
                                String studentID = value.studentID;

                                System.out.println("process Grades tuple:"+studentID);

                                Iterable<Map.Entry<String, Name>> entries = ctx.getBroadcastState(ruleMapStateDescriptor).immutableEntries();
                                
                                // while (!entries.iterator().hasNext()) {
                                //     System.out.println("process Grades tuple:"+studentID+"  No next: "+ studentID);
                                //     // List<Grades> stored = state.get(studentID);
                                //     // if (stored == null) {
                                //     //     stored = new ArrayList<>();
                                //     // }
                                //     // stored.add(value);
                                //     // state.put(studentID, stored);
                                //     Thread.sleep(2000);
                                // }

                                for (Map.Entry<String, Name> entry: ctx.getBroadcastState(ruleMapStateDescriptor).immutableEntries()){
                                    final String ruleName = entry.getKey();
                                    final Name name = entry.getValue();

                                    System.out.println("process Grades tuple:"+studentID + " process Name tuple:"+name.studentID);

                                    List<Grades> stored = state.get(ruleName);
                                    if (stored == null) {
                                        System.out.println("process Grades tuple:"+studentID +" " +"Store euqals Null");
                                        stored = new ArrayList<>();
                                    }

                                    System.out.println("process Grades tuple:"+studentID +" " +"Length Of Stored:" + stored.size()+"   StudentID is : " + studentID+ "   Name.StudentID is :" + name.studentID);

                                    if (!stored.isEmpty() && studentID.equals(name.studentID)) {
                                        for (Grades grades : stored) {
                                            out.collect(grades.toString()+" "+name.toString());
                                            System.out.println(grades.toString()+" "+name.toString());
                                        }
                                        stored.clear();
                                    }


                                   if ( !studentID.equals(name.studentID) ) {
                                    //    System.out.println("process Grades tuple:"+studentID +" " +"studentID is not equals to name.studentID");
                                       stored.add(value);
                                    //    System.out.println("process Grades tuple:"+studentID +" " +"After added, len of store is :" + stored.size());
                                   }else{
                                       out.collect(value.toString()+" "+name.toString());
                                       System.out.println("process Grades tuple:"+studentID +" " +"Out: " + value.toString()+" "+name.toString());
                                   }

                                    if ( stored.isEmpty() ) {
                                        state.remove(ruleName);
                                    }else {
                                        state.put(ruleName, stored);
                                        System.out.println("process Grades tuple:"+studentID +" " +"put Into the State");
                                    }

                                }
                            }
                        }
                );

        // out.map(new MapFunction<String, Object>() {
        //     @Override
        //     public Object map(String value) throws Exception {
        //         System.out.println(out);
        //         return null;
        //     }
        // });

        env.execute();
    
  }
}

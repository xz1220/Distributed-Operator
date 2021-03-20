package cn.xingzheng;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
// import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.configuration.Configuration;
import java.util.List;
import java.util.*;
import cn.xingzheng.DataType.*;
import org.apache.flink.api.common.state.ListStateDescriptor;


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

        DataStream<Name> fakeName = env.fromElements(
            new Name("001","xingzheng1"),
            new Name("002","xingzheng2"),
            new Name("003","xingzheng3"),
            new Name("004","xingzheng4"),
            new Name("005","xingzheng5"),
                new Name("006","xingzheng6"),
                new Name("007","xingzheng7"),
                new Name("008","xingzheng8"),
                new Name("009","xingzheng9"),
                new Name("010","xingzheng10"),
                new Name("011","xingzheng11")

        );

        DataStream<Grades> fakeGrades = env.fromElements(
            new Grades("001","99","98","97"),
            new Grades("002","96","45","97"),
            new Grades("003","97","98","97"),
            new Grades("004","94","98","97"),
            new Grades("005","23","23","97"),
            new Grades("006","95","56","97"),
            new Grades("007","95","56","97"),
            new Grades("008","95","56","97"),
            new Grades("009","95","56","97"),
            new Grades("010","95","56","97"),
            new Grades("011","95","56","97"),
            new Grades("012","95","56","97")
        );

        MapStateDescriptor<String, Name> ruleMapStateDescriptor = new MapStateDescriptor<>(
            "RulesBroadcastState",
            BasicTypeInfo.STRING_TYPE_INFO,
            TypeInformation.of(new TypeHint<Name>() {}));
         BroadcastStream<Name> broadCastName = fakeName
                            .broadcast(ruleMapStateDescriptor);


         KeyedStream<Grades,String> keyedGrades = fakeGrades.keyBy((Grades grades) -> grades.studentID);

         DataStream<String> out = keyedGrades
         .connect(broadCastName)
         .process(
                 new KeyedBroadcastProcessFunction<String, Grades, Name, String>() {
                    //  private MapStateDescriptor<String,List<Grades>> mapStateDescriptor =
                    //          new MapStateDescriptor<>(
                    //                  "grades",
                    //                  BasicTypeInfo.STRING_TYPE_INFO,
                    //                  new ListTypeInfo<>(Grades.class)
                    //          );
                     private MapStateDescriptor<String, Name> ruleMapStateDescriptor = new MapStateDescriptor<>(
                             "RulesBroadcastState",
                             BasicTypeInfo.STRING_TYPE_INFO,
                             TypeInformation.of(new TypeHint<Name>() {}));
                    
                     private transient ValueState<Grades> unprocessedGrades;

                    @Override
                    public void open(Configuration configuration) {
                        // unprocessedGrades = getRuntimeContext().getListState(new ListStateDescriptor<>("unprocessed", Grades.class));
                        unprocessedGrades = getRuntimeContext().getState(new ValueStateDescriptor<>("unprocessed", Grades.class));
                    }


                     @Override
                     public void processBroadcastElement(Name value, Context ctx, Collector<String> out) throws Exception {
//
                         // Every elements are proccessed in this function
                         ctx.getBroadcastState(ruleMapStateDescriptor).put(value.studentID, value);
                        //  System.out.println("processBroadcastElement:  "+value.toString());

                     }

                     @Override
                     public void processElement(Grades value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                         System.out.println("processElement: "+value.toString());
                        //  MapState<String, List<Grades>> state = getRuntimeContext().getMapState(mapStateDescriptor);
                         Iterable<Map.Entry<String, Name>> entries = ctx.getBroadcastState(ruleMapStateDescriptor).immutableEntries();

                         if (entries.iterator().hasNext()) {
                             System.out.println("processElement: "+value.toString() + "  BraodCastState: OK");
                            //  if (unprocessedGrades.get().iterator().hasNext()) {
                            //     System.out.println("processElement: "+value.toString() + "  unprocessedGrades is not empty");
                                //  while(unprocessedGrades.get().iterator().hasNext()) {
                                //     Grades grades = unprocessedGrades.get().iterator().next();
                                //     for (Map.Entry<String, Name> entry: entries) {
                                //         if (entry.getValue().studentID.equals(grades.studentID)) {
                                //             System.out.println("processElement: "+value.toString() + " ProcessMatched:" + grades.toString() + " " + entry.getValue().toString());
                                //         }
                                //     }
                                //  }
                            //  }
                            //  unprocessedGrades.clear();
                            // Grades left = unprocessedGrades.value();
                            for (Map.Entry<String, Name> entry: entries) {
                                // if (entry.getValue().studentID.equals(left.studentID)) {
                                //     System.out.println("processElement _ Left: "+value.toString() + "  unprocessedGrades is not empty");
                                // }

                                System.out.println("processElement " + value.toString() + " Process entry"+entry.getValue().toString());
                                if (entry.getValue().studentID.equals(value.studentID)) {
                                    System.out.println("processElement" + value.toString() + " Matched");
                                }
                                // unprocessedGrades.clear();

                            }

                         }else {
                            //  unprocessedGrades.update(value);
                         }

                        
                                              
                     }
                 }
         );

         out.print();
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

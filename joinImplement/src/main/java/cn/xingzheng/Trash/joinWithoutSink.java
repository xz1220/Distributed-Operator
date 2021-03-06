package cn.xingzheng.Trash;

import org.apache.flink.api.common.functions.RichMapFunction;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
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
import org.apache.flink.util.Collector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.List;
import java.util.*;
import cn.xingzheng.DataType.*;
import cn.xingzheng.Utils.*;
// import sun.lwawt.macosx.CSystemTray;

public class joinWithoutSink {
    
    public static void main(String[] args) throws Exception {
        try {
            BroadCastJoin();
            // Test();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void BroadCastWithFaker() throws Exception {
        System.out.println("This is test!");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        DataStream<NAME> fakeName = env.fromElements(
            new NAME("001","xingzheng1"),
            new NAME("002","xingzheng2"),
            new NAME("003","xingzheng3")
            // new Name("004","xingzheng4"),
            // new Name("005","xingzheng5"),
                // new Name("006","xingzheng6"),
                // new Name("007","xingzheng7"),
                // new Name("008","xingzheng8"),
                // new Name("009","xingzheng9"),
                // new Name("010","xingzheng10"),
                // new Name("011","xingzheng11")

        );

        DataStream<GRADES> fakeGrades = env.fromElements(
new GRADES("001","99","98","97"),
new GRADES("001","99","98","97"),
new GRADES("001","99","98","97"),
new GRADES("001","99","98","97"),
new GRADES("001","99","98","97"),
new GRADES("001","99","98","97"),
new GRADES("001","99","98","97"),
new GRADES("001","99","98","97"),
new GRADES("001","99","98","97"),
new GRADES("001","99","98","97"),
new GRADES("001","99","98","97"),

new GRADES("002","96","45","97"),
    new GRADES("003","97","98","97"),
    new GRADES("004","94","98","97"),
    new GRADES("005","23","23","97"),
    new GRADES("006","95","56","97"),
        new GRADES("007","95","56","97"),
new GRADES("008","95","56","97"),
new GRADES("009","95","56","97"),
new GRADES("010","95","56","97"),
new GRADES("011","95","56","97"),
new GRADES("012","95","56","97")
        );

        MapStateDescriptor<String, NAME> ruleMapStateDescriptor = new MapStateDescriptor<>(
            "RulesBroadcastState",
            BasicTypeInfo.STRING_TYPE_INFO,
            TypeInformation.of(new TypeHint<NAME>() {}));
         BroadcastStream<NAME> broadCastName = fakeName.broadcast(ruleMapStateDescriptor);


         KeyedStream<GRADES,String> keyedGrades = fakeGrades.keyBy(GRADES::getStudentID);

         DataStream<String> out = keyedGrades
         .connect(broadCastName)
         .process(
                 new KeyedBroadcastProcessFunction<String, GRADES, NAME, String>() {
                     private MapStateDescriptor<String,List<GRADES>> mapStateDescriptor =
                             new MapStateDescriptor<>(
                                     "grades",
                                     BasicTypeInfo.STRING_TYPE_INFO,
                                     new ListTypeInfo<>(GRADES.class)
                             );
                     private MapStateDescriptor<String, NAME> ruleMapStateDescriptor = new MapStateDescriptor<>(
                             "RulesBroadcastState",
                             BasicTypeInfo.STRING_TYPE_INFO,
                             TypeInformation.of(new TypeHint<NAME>() {}));
                    
                    ListState<GRADES> unprocessedGrades;

                    @Override
                    public void open(Configuration configuration) {
                        unprocessedGrades = getRuntimeContext().getListState(new ListStateDescriptor<GRADES>("unprocessed", GRADES.class));
                    }


                     @Override
                     public void processBroadcastElement(NAME value,
                                                         Context ctx,
                                                         Collector<String> out) throws Exception {
//
                         // Every elements are proccessed in this function
                         ctx.getBroadcastState(ruleMapStateDescriptor).put(value.studentID, value);
                         System.out.println("processBroadcastElement:  "+value.toString());

                     }

                     @Override
                     public void processElement(GRADES value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                         MapState<String, List<GRADES>> state = getRuntimeContext().getMapState(mapStateDescriptor);
                         String studentID = value.studentID;

                         System.out.println("process Grades tuple:"+studentID);

                        //   Iterable<Map.Entry<String, Name>> entries = ctx.getBroadcastState(ruleMapStateDescriptor).immutableEntries();
                         NAME tests = ctx.getBroadcastState(ruleMapStateDescriptor).get(studentID);

                         if (!ctx.getBroadcastState(ruleMapStateDescriptor).immutableEntries().iterator().hasNext()) {
                             System.out.println("process Grades tuple:"+studentID +" " +"broadCast is unready");
                             unprocessedGrades.add(value);
                             return ;
                         }else {
                             System.out.println("match success! :"+tests.toString()+" "+value.toString());
                             out.collect("match success! :"+tests.toString()+" "+value.toString());
                         }

                         for (GRADES grades : unprocessedGrades.get()) {
                            System.out.println("process Grades tuple:"+studentID +" " +"iterotar: "+grades.toString());

                             for (Map.Entry<String, NAME> entry: ctx.getBroadcastState(ruleMapStateDescriptor).immutableEntries()){
                                final String ruleName = entry.getKey();
                                final NAME name = entry.getValue();

                                System.out.println("In for");
                                if ( grades.studentID.equals(name.studentID) ) {
                                    out.collect(grades.toString()+" "+name.toString());
                                    System.out.println("process Grades tuple:"+studentID +" " +"Out: " + grades.toString()+" "+name.toString());
                                }

                            }
                            
                         }

                        //  unprocessedGrades.clear();   

                        //  for (Map.Entry<String, Name> entry: ctx.getBroadcastState(ruleMapStateDescriptor).immutableEntries()) {
                        //     System.out.println("process Grades tuple:"+studentID +" " + entry.getValue().toString());
                        //  }

                                              
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

    public static void BroadCastJoin() throws Exception {
        System.out.println("This is test!");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        ArrayList<String> parameter2 = new ArrayList<String>();
        parameter2.add("Name");
        ReadingHbase2 source2 = new ReadingHbase2("name", parameter2);
        DataStream<NAME> dataStream2 = env.addSource(source2);

        MapStateDescriptor<String, NAME> ruleMapStateDescriptor = new MapStateDescriptor<>(
                "RulesBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<NAME>() {}));
        BroadcastStream<NAME> broadCastName = dataStream2.broadcast(ruleMapStateDescriptor);

        ArrayList<String> parameter = new ArrayList<String>();
        parameter.add("English");
        parameter.add("Chinese");
        parameter.add("Math");
        ReadingHbase source = new ReadingHbase("gradesV1", parameter);
        DataStream<GRADES> dataStream = env.addSource(source);
        KeyedStream<GRADES,String> keyedGrades = dataStream.keyBy(GRADES::getStudentID);
        keyedGrades.print();
        // dataStream.print();


        // DataStream<String> out = keyedGrades
        //         .connect(broadCastName)
        //         .process(
        //                 new KeyedBroadcastProcessFunction<String, Grades, Name, String>() {
        //                     private MapStateDescriptor<String,List<Grades>> mapStateDescriptor =
        //                             new MapStateDescriptor<>(
        //                                     "grades",
        //                                     BasicTypeInfo.STRING_TYPE_INFO,
        //                                     new ListTypeInfo<>(Grades.class)
        //                             );
        //                     private MapStateDescriptor<String, Name> ruleMapStateDescriptor = new MapStateDescriptor<>(
        //                             "RulesBroadcastState",
        //                             BasicTypeInfo.STRING_TYPE_INFO,
        //                             TypeInformation.of(new TypeHint<Name>() {}));


        //                     @Override
        //                     public void processBroadcastElement(Name value,
        //                                                         Context ctx,
        //                                                         Collector<String> out) throws Exception {
        //                         // System.out.println("processBroadcastElement:  "+value.toString());
        //                         ctx.getBroadcastState(ruleMapStateDescriptor).put(value.studentID, value);

        //                     }

        //                     @Override
        //                     public void processElement(Grades value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
        //                         final MapState<String, List<Grades>> state = getRuntimeContext().getMapState(mapStateDescriptor);
        //                         String studentID = value.studentID;

        //                         System.out.println("process Grades tuple:"+studentID);

        //                         Iterable<Map.Entry<String, Name>> entries = ctx.getBroadcastState(ruleMapStateDescriptor).immutableEntries();

        //                         for (Map.Entry<String, Name> entry: ctx.getBroadcastState(ruleMapStateDescriptor).immutableEntries()){
        //                             final String ruleName = entry.getKey();
        //                             final Name name = entry.getValue();

        //                             System.out.println("process Grades tuple:"+studentID + " process Name tuple:"+name.studentID);

        //                             List<Grades> stored = state.get(ruleName);
        //                             if (stored == null) {
        //                                 System.out.println("process Grades tuple:"+studentID +" " +"Store euqals Null");
        //                                 stored = new ArrayList<>();
        //                             }

        //                             System.out.println("process Grades tuple:"+studentID +" " +"Length Of Stored:" + stored.size()+"   StudentID is : " + studentID+ "   Name.StudentID is :" + name.studentID);

        //                             // if (!stored.isEmpty() && studentID.equals(name.studentID)) {
        //                             if (studentID.equals(name.studentID)) {
        //                                 for (Grades grades : stored) {
        //                                     out.collect(grades.toString()+" "+name.toString());
        //                                     System.out.println(grades.toString()+" "+name.toString());
        //                                 }
        //                                 stored.clear();
        //                             }


        //                            if ( !studentID.equals(name.studentID) ) {
        //                             //    System.out.println("process Grades tuple:"+studentID +" " +"studentID is not equals to name.studentID");
        //                                stored.add(value);
        //                             //    System.out.println("process Grades tuple:"+studentID +" " +"After added, len of store is :" + stored.size());
        //                            }
        //                         //    else{
        //                         //        out.collect(value.toString()+" "+name.toString());
        //                         //        System.out.println("process Grades tuple:"+studentID +" " +"Out: " + value.toString()+" "+name.toString());
        //                         //    }

        //                             if ( stored.isEmpty() ) {
        //                                 state.remove(ruleName);
        //                             }else {
        //                                 state.put(ruleName, stored);
        //                                 System.out.println("process Grades tuple:"+studentID +" " +"put Into the State");
        //                             }

        //                         }
        //                     }
        //                 }
        //         );

        // out.map(new MapFunction<String, Object>() {
        //     @Override
        //     public Object map(String value) throws Exception {
        //         System.out.println(out);
        //         return null;
        //     }
        // });

        env.execute();
    }

    public static void Test() throws Exception {
        // Get the run-time
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<NAME> names = env.fromElements(
            new NAME("001","xingzheng1"),
            new NAME("002","xingzheng2"),
            new NAME("003","xingzheng3")
        );

        DataSet<GRADES> grades = env.fromElements(
            new GRADES("001","99","98","97"),
            new GRADES("002","96","45","97"),
            new GRADES("003","97","98","97"),
            new GRADES("004","94","98","97"),
            new GRADES("005","23","23","97"),
            new GRADES("006","95","56","97"),
            new GRADES("007","95","56","97"),
            new GRADES("008","95","56","97"),
            new GRADES("009","95","56","97"),
            new GRADES("010","95","56","97"),
            new GRADES("011","95","56","97"),
            new GRADES("012","95","56","97")
        );

        grades.map(new RichMapFunction<GRADES,String>(){
            @Override
            public void open(Configuration parameters) throws Exception {
                // 3. Access the broadcast DataSet as a Collection
                Collection<NAME> broadcastSet = getRuntimeContext().getBroadcastVariable("broadcastSetName");
            }

            @Override
            public String map(GRADES grades) throws Exception {
                Collection<NAME> broadcastSet = getRuntimeContext().getBroadcastVariable("broadcastSetName");
                for (NAME name: broadcastSet) {
                    System.out.println(name.toString());
                }
                return null;
            }
        }).withBroadcastSet(names, "broadcastSetName");

    }
}



/**
 * TODO: 
 *  Q1: 重复使用new 同一个类会导致配置的覆盖，查找为什么？
 *      目前的解决方法是，使用两个类似的类，
 */

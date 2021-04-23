package cn.xingzheng.Join;

import java.util.Collection;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import cn.xingzheng.DataType.*;
import org.apache.flink.util.Collector;

public class joinwithsink {

    public static void main(String[] args) throws Exception {
        try {
            joinWithoutSink();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void joinWithSink(String joinKey) {
        /**
         * 创建执行环境 并读取数据
         */
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        
    }
    
    public static void joinWithoutSink() throws Exception {
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

        DataSet<String> result = grades
                .flatMap(new RichFlatMapFunction<GRADES, String>() {
                    @Override
                    public void flatMap(GRADES value, Collector<String> out) throws Exception {
                        Collection<NAME> broadcastSet = getRuntimeContext().getBroadcastVariable("broadcastSetName");
                        for (NAME name: broadcastSet) {
                            if (name.studentID.equals(value.studentID)) {
                                out.collect(name.toString() + " " + value.toString());
                            }
                        }
                    }

                })
                .withBroadcastSet(names, "broadcastSetName");

        result.print();
    }
}



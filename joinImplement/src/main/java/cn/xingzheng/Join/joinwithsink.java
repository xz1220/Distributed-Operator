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
    
    public static void joinWithoutSink() throws Exception {
        // Get the run-time
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Name> names = env.fromElements(
            new Name("001","xingzheng1"),
            new Name("002","xingzheng2"),
            new Name("003","xingzheng3")
        );

        DataSet<Grades> grades = env.fromElements(
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

        DataSet<String> result = grades
                .flatMap(new RichFlatMapFunction<Grades, String>() {
                    @Override
                    public void flatMap(Grades value, Collector<String> out) throws Exception {
                        Collection<Name> broadcastSet = getRuntimeContext().getBroadcastVariable("broadcastSetName");
                        for (Name name: broadcastSet) {
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



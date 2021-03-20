package cn.xingzheng.JoinWithSink;

import java.util.Collection;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

import cn.xingzheng.DataType.*;

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

        DataSet<String> result = grades.map(new RichMapFunction<Grades,String>(){
            @Override
            public void open(Configuration parameters) throws Exception {
                // 3. Access the broadcast DataSet as a Collection
                Collection<Name> broadcastSet = getRuntimeContext().getBroadcastVariable("broadcastSetName");
            }

            @Override
            public String map(Grades grades) throws Exception {
                Collection<Name> broadcastSet = getRuntimeContext().getBroadcastVariable("broadcastSetName");
                // for (Name name: broadcastSet) {
                //     System.out.println(name.toString());
                // }
                // System.out.println(grades.toString());
                return grades.toString() + " " + broadcastSet.size(); 
            }
        }).withBroadcastSet(names, "broadcastSetName");

        grades.print();
        result.print();

    }
}

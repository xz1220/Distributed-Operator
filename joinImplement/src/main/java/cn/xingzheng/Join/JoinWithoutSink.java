package cn.xingzheng.Join;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;

import cn.xingzheng.DataType.*;
import cn.xingzheng.Utils.HbaseUtils.ReadingHbase.HbaseInputForm_inner;

import org.apache.flink.util.Collector;

public class JoinWithoutSink {
    public static void main(String[] args) throws Exception {
        try {
            joinWithoutSink("studentID");
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static void joinWithoutSink( String JoinKey) throws Exception {
        // Get the run-time
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        HashMap<String, String> parameters = new HashMap<String, String>();
        parameters.put("JoinKey", JoinKey);
        ParameterTool config = ParameterTool.fromMap(parameters);
        env.getConfig().setGlobalJobParameters(config);
        
        DataSet<NAME> names = env.fromElements(
            new NAME("002","xingzheng2"),
            new NAME("001","xingzheng1"),
            new NAME("003","xingzheng3")
        ).sortPartition("studentID", Order.ASCENDING);



        DataSet<Tuple1<NAME>> innerTableDataSet = env.createInput((new HbaseInputForm_inner()).setStartRow("001").setEndRow("007"));
        DataSet<NAME> sortedInnerTableDataSet = innerTableDataSet.map(new RichMapFunction<Tuple1<NAME>,NAME>(){
            @Override
            public NAME map(Tuple1<NAME> tuple) {
                return tuple.f0;
            }
        }).sortPartition("studentID", Order.ASCENDING);

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

        // DataSet<Tuple2<GRADES,Tuple1<NAME>>> result2 = grades.join(innerTableDataSet).where("studentID").equalTo("studentID");
        // result2.print();

        for(int i = 0; i < grades.count() ; i++) {
            grades
        }

        DataSet<String> result = grades
                .flatMap(new RichFlatMapFunction<GRADES, String>() {
                    @Override
                    public void flatMap(GRADES value, Collector<String> out) throws Exception {
                        Collection<NAME> broadcastSet = getRuntimeContext().getBroadcastVariable("broadcastSetNAME");
                        for (NAME NAME: broadcastSet) {
                            if (NAME.studentID.equals(value.studentID)) {
                                out.collect(NAME.toString() + " " + value.toString());
                            }
                        }
                    }

                })
                .withBroadcastSet(names, "broadcastSetNAME");

        result.print();
    }
}

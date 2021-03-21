package cn.xingzheng.Join;

import java.util.Collection;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import cn.xingzheng.DataType.Grades;
import cn.xingzheng.DataType.Name;

public final class joinNames extends RichFlatMapFunction<Grades,String> {


    @Override
    public void open(Configuration parameters) throws Exception {
        Collection<Name> broadcastSet = getRuntimeContext().getBroadcastVariable("Names");
    }

    @Override
    public void flatMap(Grades value, Collector<String> out)  {
        Collection<Name> broadcastSet = getRuntimeContext().getBroadcastVariable("BroadcastSet");
        for(Name name: broadcastSet) {
            if (name.studentID.equals(value.studentID)) {
                out.collect(name.toString() + " " + value.toString());
            }
        }
    }
}

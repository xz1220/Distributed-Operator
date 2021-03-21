package cn.xingzheng.Join;

import java.util.Collection;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import cn.xingzheng.DataType.GRADES;
import cn.xingzheng.DataType.NAME;

public final class joinNames extends RichFlatMapFunction<GRADES,String> {


    @Override
    public void open(Configuration parameters) throws Exception {
        Collection<NAME> broadcastSet = getRuntimeContext().getBroadcastVariable("Names");
    }

    @Override
    public void flatMap(GRADES value, Collector<String> out)  {
        Collection<NAME> broadcastSet = getRuntimeContext().getBroadcastVariable("BroadcastSet");
        for(NAME name: broadcastSet) {
            if (name.studentID.equals(value.studentID)) {
                out.collect(name.toString() + " " + value.toString());
            }
        }
    }
}

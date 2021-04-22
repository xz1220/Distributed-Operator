package cn.xingzheng.Join;

import cn.xingzheng.DataType.User;
import cn.xingzheng.Utils.HbaseUtils.ReadingHbase.HbaseInputForm_Order;
import cn.xingzheng.Utils.HbaseUtils.ReadingHbase.HbaseInputForm_User;
import cn.xingzheng.Utils.HbaseUtils.WritingHbase.HbaseOutputFormat_test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

public class JoinWithoutSink {

    public static long batch_size = 1000;

    public static void main(String[] args) throws Exception {
        try {
            joinWithoutSinkMiniBatch("studentID");
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static void joinWithoutSinkMiniBatch( String JoinKey) throws Exception {
        // Get the run-time
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        HashMap<String, String> parameters = new HashMap<String, String>();
        parameters.put("JoinKey", JoinKey);
        ParameterTool config = ParameterTool.fromMap(parameters);
        env.getConfig().setGlobalJobParameters(config);


        DataSet<Tuple1<User>> innerTableDataSet = env.createInput((new HbaseInputForm_User()));
        DataSet<User> sortedInnerTableDataSet = innerTableDataSet.map(new RichMapFunction<Tuple1<User>,User>(){
            @Override
            public User map(Tuple1<User> tuple) {
                return tuple.f0;
            }
        }).sortPartition("userid", Order.ASCENDING);

        long start_index = 1;
        long batch = 3;
        ArrayList<String> columns = new ArrayList<String>();
        columns.add("userid");
        columns.add("username");

        for( int i =0 ; i< batch; i++) {
            DataSource<Tuple1<cn.xingzheng.DataType.Order>> order =env.createInput((new HbaseInputForm_Order()).setStartRow(start_index).setEndRow(start_index+batch_size));
            start_index += batch_size;
            DataSet< cn.xingzheng.DataType.Order> outer =  order.map(new MapFunction<Tuple1<cn.xingzheng.DataType.Order>, cn.xingzheng.DataType.Order>() {
                @Override
                public cn.xingzheng.DataType.Order map(Tuple1<cn.xingzheng.DataType.Order> value) {
                    return value.f0;
                }
            });

            DataSet<String> result = outer
                    .flatMap(new RichFlatMapFunction< cn.xingzheng.DataType.Order, String>() {
                        @Override
                        public void flatMap(cn.xingzheng.DataType.Order value, Collector<String> out) throws Exception {
                            Collection<User> broadcastSet = getRuntimeContext().getBroadcastVariable("broadcastSetNAME");
                            for (User user: broadcastSet) {
                                if (user.userid.equals(value.userid)) {
                                    out.collect(user.toString());
                                    break;
                                }
                            }
                        }

                    })
                    .withBroadcastSet(sortedInnerTableDataSet, "broadcastSetNAME");
            result.print();
            // result.writeAsText("/home/xingzheng/output/joinWithoutSink_Test/batch_"+i, FileSystem.WriteMode.OVERWRITE);
            // result.output((new HbaseOutputFormat_test()).setTableName("OutputV1").setColumns(columns));
        }

        env.execute();

    }
}

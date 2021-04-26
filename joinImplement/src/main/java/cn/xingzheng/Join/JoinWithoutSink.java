package cn.xingzheng.Join;

import cn.xingzheng.DataType.User;
import cn.xingzheng.Utils.HbaseUtils.ReadingHbase.HbaseInputForm_Order;
import cn.xingzheng.Utils.HbaseUtils.ReadingHbase.HbaseInputForm_User;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

public class JoinWithoutSink {

    public long batch_size = 1000;

    public JoinWithoutSink setBatchSize(long batch_size) {
        this.batch_size = batch_size;
        return this;
    }

    public static void testForSortPartion() throws Exception {
                /**
         * 创建运行时环境，批处理环境
         */
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

         /**
         * 读取小表，并进行排序
         */
        DataSet<Tuple1<User>> innerTableDataSet = env.createInput((new HbaseInputForm_User()));
        DataSet<User> sortedInnerTableDataSet = innerTableDataSet.map(new RichMapFunction<Tuple1<User>,User>(){
            @Override
            public User map(Tuple1<User> tuple) {
                return tuple.f0;
            }
        }).sortPartition("userid", Order.ASCENDING);
        
        /**
         * 确认是否sort分区成功
         */
        DataSet<User> TestforUser = sortedInnerTableDataSet.map(new MapFunction<User, User>(){

            @Override
            public User map(User value) throws Exception {
                // TODO Auto-generated method stub
                return null;
            }
            
        });

        sortedInnerTableDataSet.print();
        env.execute();
    }
    
    public void joinWithoutSinkMiniBatch( String JoinKey, long startIndex, long endIndex) throws Exception {
        /**
         * 创建运行时环境，批处理环境
         */
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        /**
         * TODO 尝试使用配置类，失败，需要进行测试
         */
        HashMap<String, String> parameters = new HashMap<String, String>();
        parameters.put("JoinKey", JoinKey);
        ParameterTool config = ParameterTool.fromMap(parameters);
        env.getConfig().setGlobalJobParameters(config);

        /**
         * 读取小表，并进行排序
         */
        DataSet<Tuple1<User>> innerTableDataSet = env.createInput((new HbaseInputForm_User()));
        DataSet<User> sortedInnerTableDataSet = innerTableDataSet.map(new RichMapFunction<Tuple1<User>,User>(){
            @Override
            public User map(Tuple1<User> tuple) {
                return tuple.f0;
            }
        }).sortPartition("userid", Order.ASCENDING);
         
        /**
         * 根据startIndex & endIndex 生成所需要的batch 数量
         */
        long start_index = 1;
        long batch = 3;
        ArrayList<String> columns = new ArrayList<String>();
        columns.add("userid");
        columns.add("username");

        for( int i =0 ; i< batch; i++) {
            DataSource<Tuple1<cn.xingzheng.DataType.Order>> order =env.createInput((new HbaseInputForm_Order()).setStartRow(start_index).setEndRow(start_index+batch_size));
            // DataSource<Tuple1<cn.xingzheng.DataType.Order>> order =env.createInput((new HbaseInputForm_Order()));            
            start_index += batch_size;
            DataSet< cn.xingzheng.DataType.Order> outer =  order.map(new MapFunction<Tuple1<cn.xingzheng.DataType.Order>, cn.xingzheng.DataType.Order>() {
                @Override
                public cn.xingzheng.DataType.Order map(Tuple1<cn.xingzheng.DataType.Order> value) {
                    return value.f0;
                }
            }).sortPartition("userid", Order.ASCENDING);

            DataSet<String> result = outer
                    .flatMap(new RichFlatMapFunction< cn.xingzheng.DataType.Order, String>() {
                        @Override
                        public void flatMap(cn.xingzheng.DataType.Order value, Collector<String> out) throws Exception {
                            Collection<User> broadcastSet = getRuntimeContext().getBroadcastVariable("broadcastSetNAME");
                            for (User user: broadcastSet) {
                                if (user.userid.equals(value.userid)) {
                                    out.collect(user.toString() + " " + value.toString());
                                    break;
                                }else if (user.userid.compareTo(value.userid) > 0) {
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

    public static void main(String[] args) throws Exception {
        try {
            // joinWithoutSinkMiniBatch("studentID");
            testForSortPartion();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
}

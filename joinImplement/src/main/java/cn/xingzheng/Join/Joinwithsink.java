package cn.xingzheng.Join;

import cn.xingzheng.DataType.CaseID;
import cn.xingzheng.DataType.Order;
import cn.xingzheng.Utils.HbaseUtils.Base.HBaseOperator;
import cn.xingzheng.Utils.HbaseUtils.Base.HbaseBaseUtil;
import cn.xingzheng.Utils.HbaseUtils.ReadingHbase.HbaseInputForm_Case;
import cn.xingzheng.Utils.HbaseUtils.ReadingHbase.HbaseInputForm_Order;
import cn.xingzheng.Utils.HbaseUtils.ReadingHbase.HbaseInputForm_String;
import cn.xingzheng.Utils.HbaseUtils.ReadingHbase.HbaseInputForm_String2;
import cn.xingzheng.Utils.HbaseUtils.WritingHbase.HbaseOutputFormat_test;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;

import java.util.ArrayList;
import java.util.Collection;

public class Joinwithsink {

    public long batch_size = 50000;

    /**
     * setBatchSize is defined to describe how many datas will be loaded into the memory in one time.
     * The value is strongly related to you memory
     */
    public Joinwithsink setBathSize(long size) {
        this.batch_size = size;
        return this;
    }


    public void readAndSinkTemp() throws Exception {
        /**
         * 创建执行环境
         * 临时表名： T1, T2
         */
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String tempTable1 = "t1";
        String tempTable2 = "t2";

        /**
         * 如果存在，删除表
         */
        org.apache.hadoop.conf.Configuration config = HbaseBaseUtil.getConfiguration();
        TableName tableName1 =TableName.valueOf(tempTable1);
        TableName tableName2 = TableName.valueOf(tempTable2);

        try {
            Connection connection = ConnectionFactory.createConnection(config);
            HTable table1 = (HTable) connection.getTable(tableName1);
            HTable table2 = (HTable) connection.getTable(tableName2);
            if (table1 != null ) {
                System.out.println(tempTable1 + "has exits! Delete it !");
                HBaseOperator.deleteTable(tempTable1);
            }
            if (table2 != null) {
                System.out.println(tempTable2 + "has exits! Delete it !");
                HBaseOperator.deleteTable(tempTable2);
            }
        }catch (Exception e) {
            e.printStackTrace();
        }

        /**
         * 读取第一个大表并写回Hbase
         */

        long batch = 2;
        // 设置columns
        ArrayList<String> columns = new ArrayList<String>();
        columns.add("caseID");
        columns.add("order");

        /**
         * 设置temp表的column
         */
        ArrayList<String> tempOrderColumn = new ArrayList<String>();
        tempOrderColumn.add("order");
        tempOrderColumn.add("userId");

        for (int i = 0 ; i < batch; i ++){
            System.out.println("Current Batch : " + i);
            /**
             * 读取数据，
             */
            DataSource<Tuple1<CaseID>> caseData = env.createInput((new HbaseInputForm_Case()).setStartRow(i*batch_size).setEndRow((i+1)*batch_size));
            HbaseInputForm_Order hbaseInputForm_Order = (new HbaseInputForm_Order()).setStartRow(i*batch_size).setEndRow((i+1)*batch_size);
            // hbaseInputForm_Order.configure(OrderConfig);
            DataSource<Tuple1<Order>> orderData = env.createInput(hbaseInputForm_Order);

            /**
             * 将Tuple1转化为String
             */
            DataSet<String> result = caseData.map(new MapFunction<Tuple1<CaseID>, String>(){

                @Override
                public String map(Tuple1<CaseID> value) throws Exception {
                    // TODO Auto-generated method stub
                    //  System.out.println(value.f0.toString());
                    System.out.println(tempTable1 + ": " + value.f0.toString());
                    return value.f0.toString();
                }

            });

            /**
             * 生成Order中间表
             */
            DataSet<String> orderSink = orderData.map(
                    new MapFunction<Tuple1<Order>, String>() {
                        @Override
                        public String map(Tuple1<Order> value) throws Exception {
                            // System.out.println(value.f0.toString());
                            System.out.println(tempTable2 + ": " + value.f0.toString());
                            return value.f0.toString();
                        }
                    }
            );

            /**
             * sink中间表进入Hbase
             */
            result.output((new HbaseOutputFormat_test()).setTableName(tempTable1).setColumns(columns).setRowkeyID(0));
            orderSink.output((new HbaseOutputFormat_test()).setTableName(tempTable2).setColumns(tempOrderColumn).setRowkeyID(0));
        }

        env.execute();

    }

    public void readTempAndJoin() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        /**
        * 创建运行时批处理环境
         */
        long New_Batch = HBaseOperator.maxOrder / batch_size;
        for (long i = 0; i <= 2; i++) {
            String startRowKey = HBaseOperator.generateOrder(i*batch_size);
            String endRowKey = HBaseOperator.generateOrder((i+1)*batch_size);
            
            System.out.println("batch: " + i + " StartRowKey: " + startRowKey + "  EndRowKey:" + endRowKey);

            DataSource<Tuple2<String, String>> tempCase = env.createInput((new HbaseInputForm_String()).setStartRow(startRowKey).setEndRow(endRowKey));
            DataSource<Tuple2<String, String>> tempOrder = env.createInput((new HbaseInputForm_String2()).setStartRow(startRowKey).setEndRow(endRowKey));
           
            DataSet<String> resultUsingMap = tempOrder.flatMap(new  RichFlatMapFunction<Tuple2<String, String>, String>(){

                @Override
                public void flatMap(Tuple2<String, String> value, Collector<String> out) throws Exception {
                    // TODO Auto-generated method stub
                    Collection<Tuple2<String, String>> broadcastVariable= getRuntimeContext().getBroadcastVariable("tempCase");
                    
                    // System.out.println(value);
                    for (Tuple2<String, String> key : broadcastVariable) {
                        // System.out.println("JoinKey : " + key.f0 + "  Case:" + key.f1 + "  Order:" + value.f1);
                        if (key.f0.equals(value.f0)) {
                            // System.out.println("JoinKey : " + key.f0 + "  Case:" + key.f1 + "  Order:" + value.f1);
                            out.collect("JoinKey : " + key.f0 + "  Case:" + key.f1 + "  Order:" + value.f1);
                            break;
                        }else if (key.f0.compareTo(value.f0) >0) {
                            break;
                        }
                    }
                }

            }).withBroadcastSet(tempCase, "tempCase");

            resultUsingMap.print();
//           result.print();
        //    test.print();
       }
       
    //    env.execute();
    }

    public void joinWithSink_nativeJoinFunction() throws Exception{

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        /**
        * 创建运行时批处理环境
         */
        long New_Batch = HBaseOperator.maxOrder / batch_size;
        for (long i = 0; i <= 2; i++) {
            String startRowKey = HBaseOperator.generateOrder(i*batch_size);
            String endRowKey = HBaseOperator.generateOrder((i+1)*batch_size);
            
            System.out.println("batch: " + i + " StartRowKey: " + startRowKey + "  EndRowKey:" + endRowKey);

            DataSource<Tuple2<String, String>> tempCase = env.createInput((new HbaseInputForm_String()).setStartRow(startRowKey).setEndRow(endRowKey));
            DataSource<Tuple2<String, String>> tempOrder = env.createInput((new HbaseInputForm_String2()).setStartRow(startRowKey).setEndRow(endRowKey));
           

           DataSet<String> result = tempOrder.join(tempCase)
                              .where(0)
                              .equalTo(0)
                              .with(new FlatJoinFunction<Tuple2<String,String>,Tuple2<String,String>,String>(){

                                  @Override
                                  public void join(Tuple2<String, String> first, Tuple2<String, String> second,
                                          Collector<String> out) throws Exception {
                                          out.collect(first.f1 + " " + second.f1);
                                  }

                              });
            result.print();
       }
       
    //    env.execute();
    }
    
    public void main(String[] args) throws Exception {
        try {
//            joinWithSink("test");
            // readAndSinkTemp();
            readTempAndJoin();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
}



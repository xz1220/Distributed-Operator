package cn.xingzheng.HbaseOnFlink;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.ArrayList;

/**
 * @Author: Yang JianQiu
 * @Date: 2019/2/22 14:38
 * 写入HBase提供两种方式
 * 第二种：实现OutputFormat接口
 */
public class HBaseOutputFormatJava implements OutputFormat<String> {
    private static String zkServer = "172.27.0.7";
    private static String port = "2181";
    private static TableName tableName = TableName.valueOf("test");
    private static final String cf1 = "cf1";
    private Connection conn = null;
    private BufferedMutator mutator;
    private int count;
    private Table table;

    /**
     * 配置输出格式。此方法总是在实例化输出格式上首先调用的
     * @param configuration
     */
    @Override
    public void configure(Configuration configuration) {

    }

    /**
     * 用于打开输出格式的并行实例，所以在open方法中我们会进行hbase的连接，配置，建表等操作。
     * @param i
     * @param i1
     * @throws IOException
     */
    @Override
    public void open(int i, int i1) throws IOException {
        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();

        config.set(HConstants.ZOOKEEPER_QUORUM, zkServer);
        config.set(HConstants.ZOOKEEPER_CLIENT_PORT, port);
        config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000);
        config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000);
        conn = ConnectionFactory.createConnection(config);

        TableName tableName = TableName.valueOf("test");
        // BufferedMutatorParams params = new BufferedMutatorParams(tableName);
        // //设置缓存1m，当达到1m时数据会自动刷到hbase
        // params.writeBufferSize(1024 * 1024); //设置缓存的大小
        // mutator = conn.getBufferedMutator(params);
        // count = 0;
        try {
            table = conn.getTable(tableName);
            if (table == null) {
                System.out.println("Table is null");
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 用于将数据写入数据源，所以我们会在这个方法中调用写入hbase的API
     * @param record
     * @throws IOException
     */
    @Override
    public void writeRecord(String record) throws IOException {
        String[] array = record.split(",");
        System.out.println(array);
        Put put = new Put(Bytes.toBytes(array[0]));
        put.addColumn(Bytes.toBytes(cf1), Bytes.toBytes("name"), Bytes.toBytes(array[1]));
        put.addColumn(Bytes.toBytes(cf1), Bytes.toBytes("age"), Bytes.toBytes(array[2]));
        // mutator.mutate(put);
        // //每满2000条刷新一下数据
        // if (count >= 2000){
        //     mutator.flush();
        //     count = 0;
        // }
        // count = count + 1;

        table.put(put);
    }

    /**
     * 这个就是关闭数据源的连接
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        if (conn != null){
            conn.close();
        }
    }
}

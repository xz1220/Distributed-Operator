package cn.xingzheng.Utils.HbaseUtils.WritingHbase;

import cn.xingzheng.Utils.HbaseUtils.Base.HBaseOperator;
import cn.xingzheng.Utils.HbaseUtils.Base.HbaseBaseUtil;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

public class HbaseOutputFormat_test implements OutputFormat<String> {

    private String tableNameString = "OutputV2";
    private Connection conn = null;
    private BufferedMutator mutator;
    private int count;
    private ArrayList<String> columns = null ;
    private int rowKeyID = 0;

    public HbaseOutputFormat_test setRowkeyID(int id) {
        this.rowKeyID = id;
        return this;
    }

    public HbaseOutputFormat_test setColumns(ArrayList<String> columns) {
        this.columns = columns;
        return this;
    }

    public HbaseOutputFormat_test setTableName(String tableName) {
        this.tableNameString = tableName;
        return this;
    }
    
    @Override
    public void configure(Configuration parameters) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        // TODO Auto-generated method stub

        org.apache.hadoop.conf.Configuration config = HbaseBaseUtil.getConfiguration();
        conn = ConnectionFactory.createConnection(config);
        TableName tableName = TableName.valueOf(tableNameString);
        BufferedMutatorParams params = new BufferedMutatorParams(tableName);
        try {
            if ( tableName == null || columns == null ) {
                throw new Exception();
            }

            if (!conn.getAdmin().tableExists(tableName)) {
                HBaseOperator hbaseOperator = new HBaseOperator();
                HBaseOperator.createTable(tableName, columns);
            }
            
        }catch(Exception e) {
            e.printStackTrace();
        }
        //设置缓存1m，当达到1m时数据会自动刷到hbase
        params.writeBufferSize(1024 * 1024); //设置缓存的大小
        mutator = conn.getBufferedMutator(params);
        count = 0;
    }

    @Override
    public void writeRecord(String record) throws IOException {
        // TODO Auto-generated method stub
        String[] array = record.split(",");
        // System.out.println(array);
        Put put = new Put(Bytes.toBytes(array[rowKeyID]));

        for(int i = 0; i< columns.size() ; i++) {
            put.addColumn(Bytes.toBytes(columns.get(i)), Bytes.toBytes(columns.get(i).toLowerCase()), Bytes.toBytes(array[i]));
        }
        mutator.mutate(put);
        //每满2000条刷新一下数据
        if (count >= 200){
            mutator.flush();
            count = 0;
        }
        count = count + 1;
    }

    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub
        if (conn != null){
            conn.close();
        }
        
    }


}

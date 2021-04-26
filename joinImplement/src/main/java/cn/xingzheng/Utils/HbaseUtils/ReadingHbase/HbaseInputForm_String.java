package cn.xingzheng.Utils.HbaseUtils.ReadingHbase;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import cn.xingzheng.DataType.CaseID;
import cn.xingzheng.Utils.HbaseUtils.Base.CustomTableInputFormat;
import cn.xingzheng.Utils.HbaseUtils.Base.HBaseOperator;
import cn.xingzheng.Utils.HbaseUtils.Base.HbaseBaseUtil;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

import cn.xingzheng.Utils.HbaseUtils.Base.CustomTableInputFormat;
import cn.xingzheng.Utils.HbaseUtils.Base.HbaseBaseUtil;

public class HbaseInputForm_String extends CustomTableInputFormat<Tuple2<String, String>>{

    private String tableNameString = "t1";
    private String startRow = null;
    private String endRow = null;

    public HbaseInputForm_String setStartRow(String startRow) {
        this.startRow = startRow;
        return this;
    }

    public HbaseInputForm_String setEndRow(String endRow) {
        this.endRow = endRow;
        return this;
    }

    @Override
    public void configure(Configuration parameters) {
        // TODO Auto-generated method stub
        org.apache.hadoop.conf.Configuration config = HbaseBaseUtil.getConfiguration();
        // ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        // String tableNameString = parameterTool.getRequired("innerTbale");
        // TableName tableName =TableName.valueOf(parameters.getString("tableName"," "));
        TableName tableName =TableName.valueOf(tableNameString);

        try {
            Connection connection = ConnectionFactory.createConnection(config);
            table = (HTable) connection.getTable(tableName);
            if (table == null) {
                throw new IOException(tableName + " is not exit!");
            }
            scan  = new Scan();
            if (startRow != null) {
                scan.withStartRow(Bytes.toBytes(startRow));
            }
            if (endRow != null) {
                scan.withStopRow(Bytes.toBytes(endRow));
            }
            
        }catch (Exception e) {
            e.printStackTrace();
        }
        
    }

    /**
     * Returns an instance of Scan that retrieves the required subset of records from the HBase table.
     *
     * @return The appropriate instance of Scan for this usecase.
     */
    @Override
    protected Scan getScanner() {
        return scan;
    }

    @Override
    protected String getTableName() {
        // TODO Auto-generated method stub
        return tableNameString;
    }

    @Override
    protected Tuple2<String, String> mapResultToTuple(Result r) {
        String rowKey = Bytes.toString(r.getRow());

        // ArrayList<String> values = new ArrayList<String>();
        String values = " " ;
        for (Cell cell : r.listCells()){
            String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            // values.add(value);
            values += value + " " ;
        } 
        
        return Tuple2.of(rowKey, values);
    }

    
}

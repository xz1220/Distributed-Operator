package cn.xingzheng.Utils.HbaseUtils.ReadingHbase;

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

public class HbaseInputForm_Case extends CustomTableInputFormat<Tuple1<CaseID>> {

    private String tableNameString = "Case";
    private String startRow = null;
    private String endRow = null ;

    public HbaseInputForm_Case setStartRow(long startRow) {
        this.startRow = HBaseOperator.generateRowkey(HBaseOperator.maxIndex, startRow);
        return this;
    }

    public HbaseInputForm_Case setEndRow(long endRow) {
        this.endRow = HBaseOperator.generateRowkey(HBaseOperator.maxIndex, endRow);
        return this ;
    }

    @Override
    public void configure(Configuration parameters) {

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

    @Override
    protected Scan getScanner() {
        // TODO Auto-generated method stub
        return scan;
    }

    @Override
    protected String getTableName() {
        // TODO Auto-generated method stub
        return tableNameString;
    }

    @Override
    protected Tuple1<CaseID> mapResultToTuple(Result r) {
        // String rowKey = Bytes.toString(r.getRow());

        ArrayList<String> values = new ArrayList<String>();
        for (Cell cell : r.listCells()){
            String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            values.add(value);
        }
        
        return Tuple1.of(new CaseID(values.get(0), values.get(1)));
    }
    
}

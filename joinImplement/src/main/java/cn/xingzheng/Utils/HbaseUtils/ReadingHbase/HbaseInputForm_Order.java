package cn.xingzheng.Utils.HbaseUtils.ReadingHbase;

import cn.xingzheng.DataType.Order;
import cn.xingzheng.Utils.HbaseUtils.Base.CustomTableInputFormat;
import cn.xingzheng.Utils.HbaseUtils.Base.HBaseOperator;
import cn.xingzheng.Utils.HbaseUtils.Base.HbaseBaseUtil;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

public class HbaseInputForm_Order extends CustomTableInputFormat<Tuple1<Order>> {

    private String tableNameString = "Order";
    private String startRow = null;
    private String endRow = null;

    public HbaseInputForm_Order setStartRow(long startRow) {
        this.startRow = HBaseOperator.generateRowkey(HBaseOperator.maxIndex, startRow);
        return this;
    }

    public HbaseInputForm_Order setEndRow(long endRow) {
        this.endRow = HBaseOperator.generateRowkey(HBaseOperator.maxIndex,endRow);
        return this;
    }

    @Override
    public void configure(Configuration parameters) {

        org.apache.hadoop.conf.Configuration config = HbaseBaseUtil.getConfiguration();
        // ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        // String tableNameString = parameterTool.getRequired("innerTbale");
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

    /**
     * What table is to be read.
     * Per instance of a TableInputFormat derivative only a single tablename is possible.
     *
     * @return The name of the table
     */
    @Override
    protected String getTableName() {
        return tableNameString;
    }

    /**
     * The output from HBase is always an instance of {@link Result}.
     * This method is to copy the data in the Result instance into the required {@link Tuple}
     *
     * @param r The Result instance from HBase that needs to be converted
     * @return The appropriate instance of {@link Tuple} that contains the needed information.
     */
    @Override
    protected Tuple1<Order> mapResultToTuple(Result r) {
        String rowKey = Bytes.toString(r.getRow());
        ArrayList<String> values = new ArrayList<String>();
        for (Cell cell : r.listCells()){
            String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            values.add(value);
        }
        
        return Tuple1.of(new Order(rowKey, values.get(0),values.get(1)));
    }
    
}

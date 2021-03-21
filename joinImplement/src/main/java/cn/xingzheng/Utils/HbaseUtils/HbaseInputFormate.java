package cn.xingzheng.Utils.HbaseUtils;

import cn.xingzheng.DataType.Name;
import cn.xingzheng.DataType.Names;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

public class HbaseInputFormate extends ComposableInputFormat<Tuple1<Name>> {

    @Override
    public void configure(Configuration parameters) {

        // org.apache.hadoop.conf.Configuration config = HbaseBaseUtil.getConfiguration();
        // Connection connection = ConnectionFactory.createConnection(config);
        // TableName tableName = new TableName.valueOf("");

        HTable table = createTable();
        if (table != null) {
            Scan scan = getScanner();
        }
    }

    public HTable createTable() {
        return null;
    }


    /**
     * Returns an instance of Scan that retrieves the required subset of records from the HBase table.
     *
     * @return The appropriate instance of Scan for this usecase.
     */
    @Override
    protected Scan getScanner() {
        return null;
    }

    /**
     * What table is to be read.
     * Per instance of a TableInputFormat derivative only a single tablename is possible.
     *
     * @return The name of the table
     */
    @Override
    protected String getTableName() {
        return null;
    }

    /**
     * The output from HBase is always an instance of {@link Result}.
     * This method is to copy the data in the Result instance into the required {@link Tuple}
     *
     * @param r The Result instance from HBase that needs to be converted
     * @return The appropriate instance of {@link Tuple} that contains the needed information.
     */
    @Override
    protected Names mapResultToTuple(Result r) {
        return null;
    }
}

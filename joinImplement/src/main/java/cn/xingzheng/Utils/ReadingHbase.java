package cn.xingzheng.Utils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.*;

public class ReadingHbase extends RichSourceFunction<Tuple2< String, String>>{

    private final Logger logger = LoggerFactory.getLogger(ReadingHbase.class);
    private Connection conn = null;
    private Table table = null;
    private Scan scan = null;
    private String tableName = null;
    private ArrayList<String> columnNames = null;
    private String startRowkey = null;
    private String endRowkey = null;

    public ReadingHbase(String table , ArrayList<String> columnFamilys) {
        tableName = table;
        columnNames = columnFamilys;
    }

    public void setParameters(String table , ArrayList<String> columnFamilys) {
        tableName = table;
        columnNames = columnFamilys;
    }
    
    @Override
    public void open(Configuration configuration) throws Exception {
        // configuration
        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
        config.set(HConstants.ZOOKEEPER_QUORUM, "127.0.0.1");
        config.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181");
        config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000);
        config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000);

        // 创建链接
        conn = ConnectionFactory.createConnection(config);
        table = conn.getTable(TableName.valueOf(tableName));
        scan = new Scan();
        // scan.withStartRow(Bytes.toBytes("1001"));
        // scan.withStopRow(Bytes.toBytes("1004"));
        for (String columnName: columnNames) {
            scan.addFamily(Bytes.toBytes(columnName));
        }

    }

    @Override
    public void run(SourceContext<Tuple2<String, String>> sourceContext) throws Exception {
        ResultScanner rs = table.getScanner(scan);
        Iterator<Result> iterator = rs.iterator();
        while (iterator.hasNext()){
            Result result = iterator.next();
            String rowKey = Bytes.toString(result.getRow());
            StringBuffer sb = new StringBuffer();
            
            for (Cell cell: result.listCells()){
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                sb.append(value).append(",");
            }
            String valueString = sb.replace(sb.length() - 1, sb.length(), "").toString();
            Tuple2<String, String> tuple2 = new Tuple2<>();
            tuple2.setFields(rowKey, valueString);
            sourceContext.collect(tuple2);
        }
    }

    @Override
    public void cancel() {
        try {
            if (table != null){
                table.close();
            }
            if (conn != null){
                conn.close();
            }
        } catch (IOException e) {
            logger.error("Close HBase Exception:", e.toString());
        }
    }
}




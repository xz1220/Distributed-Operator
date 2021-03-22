package cn.xingzheng.Utils.HbaseUtils.Base;

import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.lang.*;

public class HBaseOperator {

    /**
     * 申明静态配置
     */
    static Configuration conf = null;
    static Connection conn = null;
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "127.0.0.1");
        try {
            conn = ConnectionFactory.createConnection(conf);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 创建只有一个列簇的表
     * @throws Exception
     */
    public static void createTable() throws Exception{
        Admin admin = conn.getAdmin();
        if (!admin.tableExists(TableName.valueOf("test"))){
            TableName tableName = TableName.valueOf("test");
            //表描述器构造器
            TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tableName);
            //列族描述器构造器
            ColumnFamilyDescriptorBuilder cdb = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("user"));
            //获得列描述器
            ColumnFamilyDescriptor cfd = cdb.build();
            //添加列族
            tdb.setColumnFamily(cfd);
            //获得表描述器
            TableDescriptor td = tdb.build();
            //创建表
            admin.createTable(td);

        }else {
            System.out.println("表已存在");
        }
        //关闭连接
    }

    /**
     * 创建表（包含多个列族）
     * @param tableName
     * @param columnFamilys
     * @throws Exception
     */
    public static void createTable(TableName tableName, String[] columnFamilys) throws Exception{
        Admin admin = conn.getAdmin();
        if (!admin.tableExists(tableName)){
            //表描述构造器
            TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tableName);
            //列族描述构造器
            ColumnFamilyDescriptorBuilder cdb;
            //获得列描述器
            ColumnFamilyDescriptor cfd;
            for (String columnFamily: columnFamilys){
                cdb = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily));
                cfd = cdb.build();
                //添加列族
                tdb.setColumnFamily(cfd);
            }
            //获得表描述器
            TableDescriptor td = tdb.build();
            //创建表
            admin.createTable(td);
        }else {
            System.out.println("表已存在！");
        }
        //关闭链接
    }

    /**
     * 添加数据（多个rowKey，多个列族，适合由固定结构的数据）
     * @param tableName
     * @param list
     * @throws Exception
     */
    public static void insertMany(TableName tableName, List<Map<String, Object>> list) throws Exception{
        List<Put> puts = new ArrayList<Put>();
        //Table负责跟记录相关的操作如增删改查等
        Table table = conn.getTable(tableName);
        if (list != null && list.size() > 0){
            for (Map<String, Object> map: list){
                Put put = new Put(Bytes.toBytes(map.get("rowKey").toString()));
                put.addColumn(Bytes.toBytes(map.get("columnFamily").toString()), Bytes.toBytes(map.get("columnName").toString()), Bytes.toBytes(map.get("columnValue").toString()));
                puts.add(put);
            }
        }
        table.put(puts);
        table.close();
        System.out.println("add data Success!");
    }

    /**
     * 构造插入数据
     */

     public static Map<String,Object> insertValueFactory(String rowKey, String columnFamily, String columnName, String columnValue) throws Exception {
         Map<String,Object> map=new HashMap<String,Object>();
         map.put("rowKey",rowKey);
         map.put("columnFamily",columnFamily);
         map.put("columnName",columnName);
         map.put("columnValue",columnValue);
         return map;
     }

    /**
     * 添加数据（多个rowKey，多个列族）
     * @throws Exception
     */
    public static void insertMany() throws Exception{
        Table table = conn.getTable(TableName.valueOf("test"));
        List<Put> puts = new ArrayList<Put>();
        Put put1 = new Put(Bytes.toBytes("rowKey1"));
        put1.addColumn(Bytes.toBytes("user"), Bytes.toBytes("name"), Bytes.toBytes("wd"));

        Put put2 = new Put(Bytes.toBytes("rowKey2"));
        put2.addColumn(Bytes.toBytes("user"), Bytes.toBytes("age"), Bytes.toBytes("25"));

        Put put3 = new Put(Bytes.toBytes("rowKey3"));
        put3.addColumn(Bytes.toBytes("user"), Bytes.toBytes("weight"), Bytes.toBytes("60kg"));

        Put put4 = new Put(Bytes.toBytes("rowKey4"));
        put4.addColumn(Bytes.toBytes("user"), Bytes.toBytes("sex"), Bytes.toBytes("男"));

        puts.add(put1);
        puts.add(put2);
        puts.add(put3);
        puts.add(put4);
        table.put(puts);
        table.close();
    }

    /**
     * 根据RowKey , 列簇， 列名修改值
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param columnName
     * @param columnValue
     * @throws Exception
     */
    public static void updateData(TableName tableName, String rowKey, String columnFamily, String columnName, String columnValue) throws Exception{
        Table table = conn.getTable(tableName);
        Put put1 = new Put(Bytes.toBytes(rowKey));
        put1.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(columnValue));
        table.put(put1);
        table.close();
    }

    /**
     * 根据rowKey删除一行数据
     * @param tableName
     * @param rowKey
     * @throws Exception
     */
    public static void deleteData(TableName tableName, String rowKey) throws Exception{
        Table table = conn.getTable(tableName);
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
        table.close();
    }

    /**
     * 删除某一行的某一个列簇内容
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @throws Exception
     */
    public static void deleteData(TableName tableName, String rowKey, String columnFamily) throws Exception{
        Table table = conn.getTable(tableName);
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addFamily(Bytes.toBytes(columnFamily));
        table.delete(delete);
        table.close();
    }

    /**
     * 删除某一行某个列簇某列的值
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param columnName
     * @throws Exception
     */
    public static void deleteData(TableName tableName, String rowKey, String columnFamily, String columnName) throws Exception{
        Table table = conn.getTable(tableName);
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));
        table.delete(delete);
        table.close();
    }

    /**
     * 删除某一行某个列簇多个列的值
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param columnNames
     * @throws Exception
     */
    public static void deleteData(TableName tableName, String rowKey, String columnFamily, List<String> columnNames) throws Exception{
        Table table = conn.getTable(tableName);
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        for (String columnName: columnNames){
            delete.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));
        }
        table.delete(delete);
        table.close();
    }

    /**
     * 根据rowKey查询数据
     * @param tableName
     * @param rowKey
     * @throws Exception
     */
    public static void getResult(TableName tableName, String rowKey) throws Exception{
        Table table = conn.getTable(tableName);
        //获得一行
        Get get = new Get(Bytes.toBytes(rowKey));
        Result set = table.get(get);
        Cell[] cells = set.rawCells();
        for (Cell cell: cells){
            System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) + "::" +
                    Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
        }
        table.close();
    }

    /**
     * 全表扫描
     * @param tableName
     * @throws Exception
     */
    public static void scanTable(TableName tableName) throws Exception{
        Table table = conn.getTable(tableName);
        Scan scan = new Scan();
        ResultScanner rscan = table.getScanner(scan);
        for (Result rs : rscan){
            String rowKey = Bytes.toString(rs.getRow());
            System.out.println("row key :" + rowKey);
            Cell[] cells = rs.rawCells();
            for (Cell cell: cells){
                System.out.println(Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()) + "::"
                        + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) + "::"
                        + Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
            System.out.println("-------------------------------------------");
        }
    }

    //过滤器 LESS <  LESS_OR_EQUAL <=   EQUAL =   NOT_EQUAL <>   GREATER_OR_EQUAL >=   GREATER >   NO_OP 排除所有

    /**
     * rowKey过滤器
     * @param tableName
     * @throws Exception
     */
    public static void rowKeyFilter(TableName tableName) throws Exception{
        Table table = conn.getTable(tableName);
        Scan scan = new Scan();
        //str$ 末尾匹配，相当于sql中的 %str  ^str开头匹配，相当于sql中的str%
        RowFilter filter = new RowFilter(CompareOperator.EQUAL, new RegexStringComparator("Key1$"));
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result rs: scanner){
            String rowKey = Bytes.toString(rs.getRow());
            System.out.println("row key: " + rowKey);
            Cell[] cells = rs.rawCells();
            for (Cell cell: cells){
                System.out.println(Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()) + "::"
                        + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) + "::"
                        + Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
            System.out.println("--------------------------------------------");
        }
    }

    /**
     * 列值过滤器
     * @param tableName
     * @throws Exception
     */
    public static void singColumnFilter(TableName tableName) throws Exception{
        Table table = conn.getTable(tableName);
        Scan scan = new Scan();
        //下列参数分别为列族，列名，比较符号，值
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("author"), Bytes.toBytes("name"),
                CompareOperator.EQUAL, Bytes.toBytes("spark"));
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result rs: scanner){
            String rowKey = Bytes.toString(rs.getRow());
            System.out.println("row key: " + rowKey);
            Cell[] cells = rs.rawCells();
            for (Cell cell: cells){
                System.out.println(Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()) + "::"
                        + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) + "::"
                        + Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
            System.out.println("-----------------------------------------------");
        }
    }

    /**
     * 列名前缀过滤器
     * @param tableName
     * @throws Exception
     */
    public static void columnPrefixFilter(TableName tableName) throws Exception{
        Table table = conn.getTable(tableName);
        Scan scan = new Scan();
        ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes("name"));
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result rs: scanner){
            String rowKey = Bytes.toString(rs.getRow());
            System.out.println("row key:" + rowKey);
            Cell[] cells = rs.rawCells();
            for (Cell cell : cells){
                System.out.println(Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength())+"::"+Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())+"::"+
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
            System.out.println("-----------------------------------------");
        }
    }

    /**
     * 过滤器集合
     * @param tableName
     * @throws Exception
     */
    public static void filterSet(TableName tableName) throws Exception{
        Table table = conn.getTable(tableName);
        Scan scan = new Scan();
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes("author"), Bytes.toBytes("name"),
                CompareOperator.EQUAL, Bytes.toBytes("spark"));
        ColumnPrefixFilter filter2 = new ColumnPrefixFilter(Bytes.toBytes("name"));
        list.addFilter(filter1);
        list.addFilter(filter2);

        scan.setFilter(list);
        ResultScanner scanner = table.getScanner(scan);
        for (Result rs: scanner){
            String rowkey = Bytes.toString(rs.getRow());
            System.out.println("row key :"+rowkey);
            Cell[] cells  = rs.rawCells();
            for(Cell cell : cells) {
                System.out.println(Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength())+"::"+Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())+"::"+
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
            System.out.println("-----------------------------------------");
        }

    }

    public static void insterUser() throws Exception {
        TableName tableName = TableName.valueOf("User");

        String[] columnFamilys = {"Username"};
        createTable(tableName, columnFamilys);

        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();

        for(long i = 0; i< 1<<10; i ++) {
            String user = "xingzheng" + Long.toString(i);
            String rowKey = Long.toString(i);
            list.add(insertValueFactory(rowKey,"Username","username", user));
        }

        //更新到表中
        insertMany(tableName,list);
    }

    public static void insterOrder() throws Exception {
        TableName tableName = TableName.valueOf("Order");

        String[] columnFamilys = {"Order","UserID"};
        createTable(tableName, columnFamilys);
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();

        // for(long i = 0; i< 1<<10; i ++) {
        //     String rowKey = generateRowkey(1<<30, i);
        //     String order = randomOrder();
        //     String user = randomUser();
        //     // System.out.println("rowkey: " + rowKey + " Order: " + order + " user: " + user);
        //     list.add(insertValueFactory(rowKey,"Order","order", order));
        //     list.add(insertValueFactory(rowKey,"UserID","userID", user));
        // }
        // //更新到表中
        // insertMany(tableName,list);

        // list.clear();
        // for(long i = 1<<10; i< 1<<20; i ++) {
        //     String rowKey = generateRowkey(1<<30, i);
        //     String order = randomOrder();
        //     String user = randomUser();
        //     // System.out.println("rowkey: " + rowKey + " Order: " + order + " user: " + user);
        //     list.add(insertValueFactory(rowKey,"Order","order", order));
        //     list.add(insertValueFactory(rowKey,"UserID","userID", user));
        // }
        // //更新到表中
        // insertMany(tableName,list);
        // list.clear();

        list.clear();
        for(long i = (1<<21); i< (1<<22); i ++) {
            String rowKey = generateRowkey(1<<30, i);
            String order = randomOrder();
            String user = randomUser();
            // System.out.println("rowkey: " + rowKey + " Order: " + order + " user: " + user);
            list.add(insertValueFactory(rowKey,"Order","order", order));
            list.add(insertValueFactory(rowKey,"UserID","userID", user));
        }
        //更新到表中
        insertMany(tableName,list);
        list.clear();
        
        // long i = 1<<20 ;
        // while( i < 1<<25) {
        //     list.clear();
        //     System.out.println("开始插入第" + i + "条数据");
        //     for(long index = i; index < (i+ 1<<10); index+=1) {
        //         String rowKey = generateRowkey(1<<30, index);
        //         String order = randomOrder();
        //         String user = randomUser();
        //         System.out.println("rowkey: " + rowKey + " Order: " + order + " user: " + user);
        //         list.add(insertValueFactory(rowKey,"Order","order", order));
        //         list.add(insertValueFactory(rowKey,"UserID","userID", user));
        //     }
        //     System.out.println("数据生产结束");
        //     insertMany(tableName,list);
        //     i += 1<<10;
        //     System.out.println("插入第" + i + "条数据结束");
        // }

        //更新到表中
        // insertMany(tableName,list);
    }

    public static String randomOrder() {
        String order = "";
        Random random = new Random();
        for(int orderLength = 0; orderLength < 10 ; orderLength ++){
            order+=random.nextInt(10);
        }
        return order;
    }

    public static String randomUser() {
        String user = null;
        Random random = new Random();
        user = Long.toString((long)random.nextInt(1<<10));
        return user;
    }

    public static String generateRowkey(long maxth, long currentIndex) {
        int length = Long.toString(maxth).length();
        String index = Long.toString(currentIndex);
        while(index.length() < length) {
            index = "0" + index;
        }
        return index;
    }

    public static void rowCountByCoprocessor(String tablename){
        try {
           // 协处理器
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        insterOrder();
    }

}


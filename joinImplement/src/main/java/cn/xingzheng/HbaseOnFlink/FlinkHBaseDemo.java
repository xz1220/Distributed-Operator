package cn.xingzheng.HbaseOnFlink;

import cn.xingzheng.HbaseOnFlink.flinkBatchProcessing.HBaseOnFlinkBatchProcessingJava;
import cn.xingzheng.HbaseOnFlink.flinkStreaming.HBaseOnFlinkStreamingJava;

/**
 * @Author: Yang JianQiu
 * @Date: 2019/3/16 18:06
 */
public class FlinkHBaseDemo {
    
    /**
     * 读写的过程中，hbase内部需要有对应的表的存在。
     */
    public static void main(String[] args) throws Exception {
        HBaseOnFlinkBatchProcessingJava hofbp = new HBaseOnFlinkBatchProcessingJava();
        hofbp.readFromHBaseWithTableInputFormat();
        // hofbp.write2HBaseWithOutputFormat();

    //    HBaseOnFlinkStreamingJava hofs = new HBaseOnFlinkStreamingJava();
    // //    hofs.write2HBaseWithRichSinkFunction();
    //    hofs.write2HBaseWithOutputFormat();
    }
}

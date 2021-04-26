package cn.xingzheng;

import cn.xingzheng.Join.Joinwithsink;


public class main {
  public static void main(String[] args) throws Exception {
      // CommandLineParser parser = new BasicParser();
      // Options options = new Options();
      // options.addOption("h", "help", false , "Print this usage information");
      // options.addOption("f", "function", true, "utils.hbase for operating the hbase");

      // try {
      //   org.apache.commons.cli.CommandLine commandLine = parser.parse(options, args);
      //   String function = "";

      //   if (commandLine.hasOption("h")) {
      //       System.out.println("This is help!");
      //   }
  
      //   if (commandLine.hasOption("f")) {
      //         function = commandLine.getOptionValue("f");
      //         if (function.compareTo("utils") == 0) {
      //               // HBaseOperator.insertCasesForStream();;
      //         }else{
      //             System.out.println("Arguments Error");
      //         }
      //   }

      // }catch (Exception e) {
      //     e.printStackTrace();
      // }
      /**
       *
       */
//      JoinWithoutSink joinwithsink = new JoinWithoutSink();
//      joinwithsink.joinWithoutSinkMiniBatch("test");

      /**
       * 测试Join with SInk
       */
      Joinwithsink joinwithsink = new Joinwithsink();
      // joinwithsink.readAndSinkTemp();
     joinwithsink.readTempAndJoin();

  }
}

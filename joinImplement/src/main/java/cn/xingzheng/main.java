package cn.xingzheng;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import java.util.List;
import java.util.*;
import cn.xingzheng.DataType.*;
import cn.xingzheng.Utils.HbaseUtils.Base.HBaseOperator;


public class main {
  public static void main(String[] args) throws Exception {
      CommandLineParser parser = new BasicParser();
      Options options = new Options();
      options.addOption("h", "help", false , "Print this usage information");
      options.addOption("f", "function", true, "utils.hbase for operating the hbase");

      try {
        org.apache.commons.cli.CommandLine commandLine = parser.parse(options, args);
        String function = "";

        if (commandLine.hasOption("h")) {
            System.out.println("This is help!");
        }
  
        if (commandLine.hasOption("f")) {
              function = commandLine.getOptionValue("f");
              if (function.compareTo("utils") == 0) {
                    HBaseOperator.insertCasesForStream();;
              }else{
                  System.out.println("Arguments Error");
              }
        }

      }catch (Exception e) {
          e.printStackTrace();
      }
      
    
  }
}

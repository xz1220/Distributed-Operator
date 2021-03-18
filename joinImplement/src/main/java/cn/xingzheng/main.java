package cn.xingzheng;

import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import cn.xingzheng.Utils.*;

public class main {
  public static void main(String[] args) {
      CommandLineParser parser = new BasicParser();
      Options options = new Options();
      options.addOption("h", "help", false , "Print this usage information");
      options.addOption("f", "function", true, "utils.hbase for operating the hbase");

      try {
        CommandLine commandLine = parser.parse(options, args);
        String function = "";

        if (commandLine.hasOption("h")) {
            System.out.println("This is help!");
        }
  
        if (commandLine.hasOption("f")) {
              function = commandLine.getOptionValue("f");
              if (function.compareTo("utils") == 0) {
                    HBaseJavaApiDemo.insertCasesForStream();;
              }else{
                  System.out.println("Arguments Error");
              }
        }

      }catch (Exception e) {
          e.printStackTrace();
      }
    
  }
}

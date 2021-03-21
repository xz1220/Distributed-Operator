package cn.xingzheng.DataType;

import org.apache.flink.api.java.tuple.Tuple;
import scala.tools.nsc.backend.jvm.opt.BoxUnbox;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;

public class NAME implements  Serializable, Comparable<NAME> {
    
    /**
     * 
     */

     public String studentID;
     public String studentName;

     public NAME() {
         this.studentID = null;
         this.studentName = null;
     }

     public NAME(String studentID, String studentName) {
        this.studentID = studentID;
        this.studentName = studentName;
     }

     public NAME(String studentID, ArrayList<String> values) {
        this.studentID = studentID ;
        if (values.size() == 1) {
            this.studentName = values.get(0);
        }
    }

     public void setParameters(String studentID, ArrayList<String> values) {
         this.studentID = studentID ;
         if (values.size() == 1) {
             this.studentName = values.get(0);
         }
     }

     @Override
     public String toString() {
         return studentID + "," + studentName;
     }

     public int compareTo(@Nullable NAME name) {
         if (name == null) {
             return 1;
         }

         int compareResult = this.studentName.compareTo(name.studentName);
         return compareResult;
     }

     @Override
     public boolean equals(Object other) {
         return other instanceof NAME &&
            this.studentName == ((NAME) other).studentName;
     }


}

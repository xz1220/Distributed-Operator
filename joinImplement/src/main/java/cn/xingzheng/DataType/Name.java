package cn.xingzheng.DataType;

import org.apache.flink.api.java.tuple.Tuple;
import scala.tools.nsc.backend.jvm.opt.BoxUnbox;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.time.Instant;

public class Name implements  Serializable, Comparable<Name> {
    
    /**
     * 
     */

     public String studentID;
     public String studentName;

     public Name(String studentID, String studentName) {
        this.studentID = studentID;
        this.studentName = studentName;
     }

     @Override
     public String toString() {
         return studentID + "," + studentName;
     }

     public int compareTo(@Nullable Name name) {
         if (name == null) {
             return 1;
         }

         int compareResult = this.studentName.compareTo(name.studentName);
         return compareResult;
     }

     @Override
     public boolean equals(Object other) {
         return other instanceof Name &&
            this.studentName == ((Name) other).studentName;
     }


}

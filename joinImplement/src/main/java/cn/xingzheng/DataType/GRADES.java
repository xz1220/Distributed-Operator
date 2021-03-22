package cn.xingzheng.DataType;

import java.io.Serializable;
import java.util.ArrayList;

public class GRADES implements Serializable {

    public String studentID;
    public String ChineseGrade;
    public String EnglishGrade;
    public String MathGrade;

    public GRADES() {
        this.ChineseGrade = null;
        this.EnglishGrade = null;
        this.studentID = null;
        this.MathGrade = null;
    }

    public GRADES(String studentID, String ChineseGrad, String EnglishGrade, String MathGrade) {
        this.ChineseGrade = ChineseGrad;
        this.EnglishGrade = EnglishGrade;
        this.studentID = studentID;
        this.MathGrade = MathGrade;
    }

    public void setParameters(String studentID, ArrayList<String> values) {
        this.studentID = studentID;
        if (values.size() == 3) {
            this.ChineseGrade = values.get(0);
            this.EnglishGrade = values.get(1);
            this.MathGrade = values.get(2);
        }
     }


    @Override
    public String toString() {
        return studentID+ ","+ ChineseGrade+", " + EnglishGrade +", "+MathGrade;
    }

    public String getStudentID() {
        return this.studentID;
    }

}

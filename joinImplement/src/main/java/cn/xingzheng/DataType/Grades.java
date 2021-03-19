package cn.xingzheng.DataType;

import org.apache.kafka.common.protocol.types.Field;

import java.io.Serializable;

public class Grades implements Serializable {

    public String studentID;
    public String ChineseGrade;
    public String EnglishGrade;
    public String MathGrade;

    public Grades(String studentID, String ChineseGrad, String EnglishGrade, String MathGrade) {
        this.ChineseGrade = ChineseGrad;
        this.EnglishGrade = EnglishGrade;
        this.studentID = studentID;
        this.MathGrade = MathGrade;
    }


    @Override
    public String toString() {
        return studentID+ ","+ ChineseGrade+", " + EnglishGrade +", "+MathGrade;
    }

    public String getStudentID() {
        return this.studentID;
    }

}

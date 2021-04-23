package cn.xingzheng.DataType;

import java.io.Serializable;

public class CaseID implements Serializable {
    public String order;
    public String caseID;

    /** 
     * 构造函数
    */
    public CaseID() {
        this.order = null;
        this.caseID = null;
    }

    public CaseID(String order, String caseID) {
        this.order = order;
        this.caseID = caseID;
    }

    @Override
    public String toString() {
        return order + "," + caseID;
    }

}
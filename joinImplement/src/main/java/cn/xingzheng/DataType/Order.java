package cn.xingzheng.DataType;

import java.io.Serializable;

public class Order implements Serializable{
//    public String orderid;
    public String order;
    public String userid;

    public Order() {
//        this.orderid =null;
        this.order = null;
        this.userid =null;
    }

    public Order(String order, String userid) {
//        this.orderid = orderid;
        this.order = order;
        this.userid = userid;
    }

    @Override
    public String toString() {
        return  order + "," + userid;
    }
}

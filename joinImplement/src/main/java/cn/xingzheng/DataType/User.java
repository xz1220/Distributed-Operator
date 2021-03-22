package cn.xingzheng.DataType;

import java.io.Serializable;

public class User implements Serializable{
    public String userid;
    public String username;

    public User() {
        this.userid = null ;
        this.username = null ;
    }

    public User(String userid, String username) {
        this.userid = userid ;
        this.username = username ;
    }

    @Override
    public String toString() {
        return userid + "," + username;
    }
}

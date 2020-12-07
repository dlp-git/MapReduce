package com.bigdata.day03.homework;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class friendsBean implements WritableComparable<friendsBean> {
    private String user;
    private String friends;
//    比较方法
    public int compareTo(friendsBean o) {
        return this.friends.compareTo(o.friends);
    }
//    序列化
    public void write(DataOutput out) throws IOException {
        out.writeUTF(user);
        out.writeUTF(friends);
    }
//    反序列化
    public void readFields(DataInput in) throws IOException {
        this.user=in.readUTF();
        this.friends=in.readUTF();
    }

    public friendsBean() {
    }
    public friendsBean(String user, String friends) {
        this.user = user;
        this.friends = friends;
    }

    @Override
    public String toString() {
        return user +":"+friends;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getFriends() {
        return friends;
    }

    public void setFriends(String friends) {
        this.friends = friends;
    }



}

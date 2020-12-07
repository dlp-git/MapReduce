package com.bigdata.day03.definedComparableDemo;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
//    自定义比较器排序方式,继承WritableComparable类（序列化+排序）
public class stuComparaBean implements WritableComparable<stuComparaBean> {
    private String subject;
    private String name;
    private double score;

    public int compareTo(stuComparaBean o) {
        if(this.subject.compareTo(o.subject) > 0){
            return -1;
        }else if (this.subject.compareTo(o.subject) < 0){
            return 1;
        }else {
            return this.score > o.score ? -1 : 1;
        }
    }

    public stuComparaBean() {
    }
    public stuComparaBean(String subject, String name, double score) {
        this.subject = subject;
        this.name = name;
        this.score = score;
    }
//    序列化
    public void write(DataOutput out) throws IOException {
        out.writeUTF(subject);
        out.writeUTF(name);
        out.writeDouble(score);
    }
//    反序列化
    public void readFields(DataInput in) throws IOException {
        this.subject=in.readUTF();
        this.name=in.readUTF();
        this .score=in.readDouble();
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    @Override
    public String toString() {
        return subject + '\t' + name + '\t' + score;
    }



}

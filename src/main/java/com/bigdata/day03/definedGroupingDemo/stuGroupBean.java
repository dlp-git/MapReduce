package com.bigdata.day03.definedGroupingDemo;




import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//    自定义分组,继承WritableComparable类（序列化+排序）
public class stuGroupBean implements WritableComparable<stuGroupBean> {
    private String subject;
    private String name;
    private double score;

    public int compareTo(stuGroupBean o) {
        int i = this.subject.compareTo(o.subject);
        if(i == 0){//如果课程相同，根据分数二次排序
            return this.score > o.score ? -1 : 1;
        }
        return i;
    }

    public stuGroupBean() {
    }
    public stuGroupBean(String subject, String name, double score) {
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

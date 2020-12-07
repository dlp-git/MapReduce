package com.bigdata.day02.definedPartitionDemo;

import com.bigdata.day02.definedSerializeDemo.stuScoreBean;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.security.Key;
import java.util.*;


public class subjectDefinedPartition {
    /**
     * 一、根据学生科目自定义分区
     */
//    1.1.map阶段：分割得到<key（科目），value（姓名 成绩）>
    public static class subjectMap extends Mapper<LongWritable, Text, Text,Text> {
    Text subject=new Text();
    Text nameAndScore=new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] stuInfo = value.toString().split(",");
            if(stuInfo.length==3){
                subject.set(stuInfo[0]);
                nameAndScore.set(stuInfo[1]+"\t"+stuInfo[2]);
            }
            context.write(subject,nameAndScore);
        }
    }
//    2.2.分区：根据科目进行分区
    public static class subjectPartition extends Partitioner<Text,Text> {
    ArrayList<String> subjectList = new ArrayList<String>();
        public int getPartition(Text key, Text value, int numPartitions) {
            String subject=key.toString();
            if(subjectList.contains(subject)){
                return subjectList.indexOf(subject);
            }
            subjectList.add(subject);
            return subjectList.indexOf(subject);
        }
    }
    /**
     * 二、根据学生姓名自定义分区
     */
//    2.1.map阶段：输出<key(name),value(subject score)>
    public static class nameMap extends Mapper<LongWritable,Text,Text,Text>{
        Text name=new Text();
        Text subjectAndScore=new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] stuInfo = value.toString().split(",");
            if(stuInfo.length==3){
                name.set(stuInfo[1]);
                subjectAndScore.set(stuInfo[0]+"\t"+stuInfo[2]);
            }
            context.write(name,subjectAndScore);
        }
    }
//    2.2.分区：根据学生姓名分区
    public static class namePartition extends Partitioner<Text,Text>{
        ArrayList<String> nameList = new ArrayList<String>();
        public int getPartition(Text key, Text value, int numPartitions) {
            String name = key.toString();
            if(nameList.contains(name)){
                return nameList.indexOf(name);
            }
            nameList.add(name);
            return nameList.indexOf(name);
        }
    }
}

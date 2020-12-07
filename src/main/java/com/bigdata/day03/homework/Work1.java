package com.bigdata.day03.homework;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class Work1 {
    /**
     * 1、统计每门课程参考学生的平均分, ——>map out：<stu(subject,name,avgScore),NullWritable>
     * 并且按课程存入不同的结果文件，——>要求一门课程一个结果文件， partition；stu.subject;
     * 并且按平均分从高到低排序，分数保留一位小数 stuInfo.txt ——>comparable：avgScore;
     * 区内有序，需要使用comparaTo方法，而comparaTo方法是在对象中实现，所以key：stu
     */
    //Map阶段：
    public static class avgScoreMap extends Mapper<LongWritable, Text,stuInfoBean, NullWritable>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] stuInfo = value.toString().split(",");
            int sum=0;
            for (int i=2;i<stuInfo.length;i++){
                sum += Integer.parseInt(stuInfo[i]);
            }
            double avgScore = Math.round(sum / (stuInfo.length-2) * 100D) /100D;
            stuInfoBean stuInfoBean = new stuInfoBean(stuInfo[0],stuInfo[1],avgScore);
            context.write(stuInfoBean,NullWritable.get());
        }
    }
    //分区：
    public static class subjectPartiton extends Partitioner<stuInfoBean, NullWritable>{
        ArrayList<String> subjectList = new ArrayList<String>();
        public int getPartition(stuInfoBean stuInfoBean, NullWritable nullWritable, int numPartitions) {
            String subject = stuInfoBean.getSubject();
            if(subjectList.contains(subject)){
                return subjectList.indexOf(subject);
            }
            subjectList.add(subject);
            return subjectList.indexOf(subject);
        }
    }


}

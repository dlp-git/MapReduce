package com.bigdata.day02.homework;

import com.google.gson.internal.$Gson$Preconditions;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Work4 {
    /**
     * 四、统计每门课程的参考人数和课程平均分 subjectScore.txt
     * 4.1.统计每门课程所有学生每次考试的成绩/所有学生参加考试次数的和
     */
    public static class subjectMap extends Mapper<LongWritable, Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] scoreInfo = value.toString().split("\\|");
            if (scoreInfo.length==3) {
                Text subject = new Text(scoreInfo[0]);
                Text nameAndScores = new Text(scoreInfo[1] + " " + scoreInfo[2] + "|");
                context.write(subject, nameAndScores);
            }
        }
    }
    public static class subjectRedece extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int num=0;
            int sum=0;
            int totalNum=0;
            for (Text nameAndScores : values) {
                String[] nameAndScore = nameAndScores.toString().split("\\|");
                for (String ns : nameAndScore) {
                    String[] scores = ns.split(" ")[1].split(",");
                    for (String score : scores) {
                        sum+=Integer.parseInt(score);
                    }
                    totalNum+=scores.length;
                }
                num++;
            }
            double avg=Math.round(sum/totalNum*100D)/100D;
            context.write(key,new Text("参考人数:"+num+"  平均成绩:"+avg));
        }
    }

    /**
     * 四、统计每门课程的参考人数和课程平均分 subjectScore.txt
     * 4.2.统计每门课程每个学生考试的平均成绩的和/参考人数
     */
    public  static class avgScoreMap extends Mapper<LongWritable, Text,Text, DoubleWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] scoreInfo = value.toString().split("\\|");
            int num=0;
            int sum=0;
            if (scoreInfo.length==3) {
                Text subject = new Text(scoreInfo[0]);
                String[] scores = scoreInfo[2].split(",");
                for (String score : scores) {
                    sum+=Integer.parseInt(score);
                    num++;
                }
                double avg=Math.round(sum/num*100D)/100D;
                context.write(subject, new DoubleWritable(avg));
            }
        }
    }

    public static class avgScoreReduce extends Reducer<Text, DoubleWritable,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            int num=0;
            double avgScores=0;
            for (DoubleWritable avgScore : values) {
                avgScores+=Double.parseDouble(avgScore.toString());
                num++;
            }
            double avgScore=avgScores/num;
            context.write(key, new Text(":"+avgScore+" "+num+"人"));
        }
    }
}

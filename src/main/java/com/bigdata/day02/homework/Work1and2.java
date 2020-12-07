package com.bigdata.day02.homework;

import com.bigdata.day02.definedSerializeDemo.stuScoreBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class Work1and2 {
    /**
     * 一二、根据科目自定义分区，并统计每门课程平均分、最低分、最高分
     */
//    3.1.map阶段：output——><key(subject),value(score)>

    public static class avgScoreMap extends Mapper<LongWritable, Text,Text, stuScoreBean> {
        Text subject = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] stuInfo = value.toString().split(",");
            if(stuInfo.length==3){
                subject.set(stuInfo[0]);
                stuScoreBean score = new stuScoreBean(stuInfo[0],stuInfo[1],Double.parseDouble(stuInfo[2]));
                context.write(subject,score);
            }

        }
    }
    //    3.2.分区：根据科目分区
    public static class subjectPartitionAvg extends Partitioner<Text,stuScoreBean> {
        ArrayList<String> subjectList = new ArrayList<String>();
        public int getPartition(Text key, stuScoreBean value, int numPartitions) {
            String subject = key.toString();
            if(subjectList.contains(subject)){
                return subjectList.indexOf(subject);
            }
            subjectList.add(subject);
            return subjectList.indexOf(subject);
        }
    }

    //    3.3.reduce阶段：output——><key(subject),value(avgScore minScore maxScore)>
    public static class avgScoreReduce extends Reducer<Text,stuScoreBean,Text, Text> {
        Text subjectScore = new Text();
        ArrayList<Double> scoreList = new ArrayList<Double>();
        @Override
        protected void reduce(Text key, Iterable<stuScoreBean> values, Context context) throws IOException, InterruptedException {
            int i=0;
            double sumScore=0;
            for (stuScoreBean value : values) {
                double score = value.getScore();
                scoreList.add(score);
                System.out.print(value+" ");
                sumScore+=score;
                i++;
            }
            System.out.println(scoreList);
            if(i!=0){
                Collections.sort(scoreList);
                Double avgScore=Math.round(sumScore/i*100)/100.0;
                Double minScore=scoreList.get(0);
                Double maxScore=scoreList.get(i-1);
                subjectScore.set("平均分："+avgScore+"\t最高分："+maxScore+"\t最低分"+minScore);
            }
//            使用collection类获取最值
            Collections.max(scoreList);
            Collections.max(scoreList);

            context.write(key,subjectScore);
        }
    }
}

package com.bigdata.day02.homework;

import com.bigdata.day02.definedSerializeDemo.stuScoreBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

public class Work3 {
    /**
     *三、求该成绩表每门课程当中出现了相同分数的 分数，次数，及该分数姓名
     */
//    4.1.map阶段：output——><key(name),value(subject score)>
    public static class sameScoreMap extends Mapper<LongWritable, Text,Text, Text> {
        Text subjectAndScore = new Text();
        Text name = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] stuInfo = value.toString().split(",");
            if(stuInfo.length==3){
                subjectAndScore.set(stuInfo[0]+":"+stuInfo[2]);
                name.set(stuInfo[1]);
                context.write(subjectAndScore,name);
            }

        }
    }

    //    4.2.reduce阶段：output——><key(subject),value(sameScore,sameScoreNum,{name,...})>
    public static class sameScoreReduce1 extends Reducer<Text,Text, Text,Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            （1）使用ArrayList放name
            ArrayList<String> nameList = new ArrayList<String>();
            for (Text name : values) {
                nameList.add(name.toString());
            }
            if(nameList.size()>0) {
                context.write(key, new Text("人数:" + nameList.size() + " 姓名：" + nameList.toString()));
            }
//            （2）使用StringBuffer放name
            StringBuffer nameBuffer = new StringBuffer();
            int num=0;
            for (Text name : values) {
                nameBuffer.append(name.toString()).append("\t");
                num++;
            }

        }
    }

    public static class sameScoreReduce2 extends Reducer<Text, stuScoreBean, Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<stuScoreBean> values, Context context) throws IOException, InterruptedException {
            ArrayList<Double> scoreList = new ArrayList<Double>();
            HashSet<Double> scoreSet = new HashSet<Double>();
            ArrayList<String> nameList = new ArrayList<String>();
            ArrayList<String> sameList = new ArrayList<String>();
            for (stuScoreBean value : values) {
                double score = value.getScore();
                String name = value.getName();
                scoreList.add(score);
                scoreSet.add(score);
                nameList.add(name);
            }
            System.out.println(scoreSet.toString()+"|"+scoreList.toString());
            Iterator<Double> setIterator = scoreSet.iterator();
            while(setIterator.hasNext()){
                int i=0;
                Double nextSet = setIterator.next();
                ArrayList<String> sameScoreNameList = new ArrayList<String>();
                for (int index=0;index<scoreList.size();index++) {
                    if(nextSet.equals(scoreList.get(index))){
                        System.out.print(index+" ");
                        sameScoreNameList.add(nameList.get(index));
                        i++;
                    }
                }
                System.out.println();
                System.out.println(sameScoreNameList.toString());
                String same=" 分数:"+nextSet+" 姓名:"+sameScoreNameList.toString()+" 人数:"+i+"  ";
//                String same=" 分数:"+nextSet+" 人数:"+i+"  ";
                sameList.add(same);
            }

            System.out.println(sameList.toString());

            Text text = new Text(scoreList.toString() + "" + scoreSet.toString());
            context.write(key,new Text(sameList.toString()));
        }
    }
}

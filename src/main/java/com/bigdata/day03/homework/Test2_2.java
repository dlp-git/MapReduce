package com.bigdata.day03.homework;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Test2_2 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//        String s ="HK";
//        String s1 ="KH";
//        System.out.println(s1.hashCode()+" "+s1.hashCode());
        Configuration conf = new Configuration();
        Job job= Job.getInstance(conf,"friendsGroup2");

        job.setJarByClass(Work2_2.class);

        job.setMapperClass(Work2_2.friendsMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

//        job.setPartitionerClass(Work1.subjectPartiton.class);
//        指定自定义分组的类
//        job.setGroupingComparatorClass(Work2_1.friendGroup.class);

//        reducetask的数量大于等于分区数量或为1时，不会报错
//        reducetask的数量小于分区数量时，会报错（）
//          job.setNumReduceTasks(4);
        job.setReducerClass(Work2_2.friendsReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Path inPath=new Path("./friendsGroup001/part-r-00000");
//        Path outPath=new Path("./subDefPar001");
        Path outPath=new Path("./friendsGroup002");
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(outPath)){
            fs.delete(outPath,true);
        }
        FileInputFormat.addInputPath(job,inPath);
        FileOutputFormat.setOutputPath(job,outPath);

        boolean isCompletion = job.waitForCompletion(true);
        if(isCompletion){
            System.out.println("success......");
        }else{
            System.out.println("fail......");
        }
    }
}

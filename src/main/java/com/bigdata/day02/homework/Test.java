package com.bigdata.day02.homework;


import com.bigdata.day02.definedPartitionDemo.subjectDefinedPartition;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Test {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//        String s="computer|liujialing|85,86,41,75,93,42,85";
//        String[] split = s.split("\\|");
//        String[] split1 = split[2].split(",");
//        for (String s1 : split1) {
//            System.out.print(s1+" ");
//        }
//
//        for (String s1 : split) {
//            System.out.println(s1);
//        }
        Configuration conf = new Configuration();
        Job job=Job.getInstance(conf,"subjectDefinedPartition");

        job.setJarByClass(subjectDefinedPartition.class);

        job.setMapperClass(Work4.subjectMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

//        job.setPartitionerClass(subjectDefinedPartition.subjectPartitionAvg.class);

//        reducetask的数量大于等于分区数量或为1时，不会报错
//        reducetask的数量小于分区数量时，会报错（）
//        job.setNumReduceTasks(4);
        job.setReducerClass(Work4.subjectRedece.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Path inPath=new Path("./datas/subjectScore.txt");
//        Path outPath=new Path("./subDefPar001");
        Path outPath=new Path("./subParAvg003");
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

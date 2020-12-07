package com.bigdata.day02.definedSerializeDemo;


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

public class runTest {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job=Job.getInstance(conf,"subjectDefinedPartition");

        job.setJarByClass(subjectDefinedPartition.class);

        job.setMapperClass(definedSerializableDemo.serializableMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

//        job.setPartitionerClass(definedSerializableDemo.subjectPartitionAvg.class);

//        reducetask的数量大于等于分区数量或为1时，不会报错
//        reducetask的数量小于分区数量时，会报错（）
//        job.setNumReduceTasks(4);
//        job.setReducerClass(subjectDefinedPartition.avgScoreReduce.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);

        Path inPath=new Path("./datas/stuScore.txt");
//        Path outPath=new Path("./subDefPar001");
        Path outPath=new Path("./subParAvg001");
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

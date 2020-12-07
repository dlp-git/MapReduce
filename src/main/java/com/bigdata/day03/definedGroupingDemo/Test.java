package com.bigdata.day03.definedGroupingDemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Test {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job= Job.getInstance(conf,"DefinedSubjectGrouping");

        job.setJarByClass(DefinedSubjectGrouping.class);

        job.setMapperClass(DefinedSubjectGrouping.DefSubMap.class);
        job.setMapOutputKeyClass(stuGroupBean.class);
        job.setMapOutputValueClass(NullWritable.class);

//        job.setPartitionerClass(subjectDefinedPartition.namePartition.class);
//        指定自定义分组的类
        job.setGroupingComparatorClass(DefinedSubjectGrouping.DefSubGroup.class);

//        reducetask的数量大于等于分区数量或为1时，不会报错
//        reducetask的数量小于分区数量时，会报错（）
//        job.setNumReduceTasks(4);
        job.setReducerClass(DefinedSubjectGrouping.DefSubReduce.class);
        job.setOutputKeyClass(stuGroupBean.class);
        job.setOutputValueClass(NullWritable.class);

        Path inPath=new Path("./datas/stuScore.txt");
//        Path outPath=new Path("./subDefPar001");
        Path outPath=new Path("./stuGroup001");
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

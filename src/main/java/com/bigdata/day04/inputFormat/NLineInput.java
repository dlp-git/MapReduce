package com.bigdata.day04.inputFormat;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class NLineInput {
    /**
     * 默认TextInputFormat：一个block为一次切片，每次输入一行
     * NLineInputFormat：设置多行输入，n行为一个切片，每次输入n行
     * 测试文件：wc.txt
     */
    public static class wcMap extends Mapper<LongWritable, Text,Text, IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] s = value.toString().split(" ");
            for (String s1 : s) {
                context.write(new Text(s1),new IntWritable(1));
            }
        }
    }
    public static class wcReduce extends Reducer<Text, IntWritable,Text, IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int i=0;
            for (IntWritable value : values) {
                i++;
            }
            context.write(key,new IntWritable(i));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job= Job.getInstance(conf,"NLineInput");
        job.setMapperClass(wcMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setReducerClass(wcReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.setNumLinesPerSplit(job,2);
        Path inPath = new Path("./datas/wc.txt");
        Path outPath = new Path("./TextInputFormat001_NLine");
        FileInputFormat.addInputPath(job,inPath);
        FileOutputFormat.setOutputPath(job,outPath);
        FileSystem fs =FileSystem.get(conf);
        if(fs.exists(outPath)){
            fs.delete(outPath,true);
        }
        boolean isCompletion = job.waitForCompletion(true);
        if(isCompletion){
            System.out.println("success.....");
        }else{
            System.out.println("fail......");
        }
    }
}

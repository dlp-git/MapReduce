package com.bigdata.day04.inputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CombineInput {
    /**
     * CombineTextInputFormat:是
     * 可以用来优化MapReduce对小文件的处理
     * 测试文件：stuScore.txt,stuScore2.txt
     */
    public static class combineMap extends Mapper<LongWritable, Text,Text, DoubleWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] info = value.toString().split(",");
            if(info.length==3){
                context.write(new Text(info[0]),new DoubleWritable(Double.parseDouble(info[2])));
            }
        }
    }
    public static class combineReduce extends Reducer<Text, DoubleWritable,Text, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            int num=0;
            double sum=0;
            for (DoubleWritable score : values) {
                sum+=Double.parseDouble(score.toString());
                num++;
            }
            double avg=sum/num;
            context.write(key,new DoubleWritable(avg));
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job=Job.getInstance(conf,"CombineTextInputFormat");
        job.setJarByClass(CombineInput.class);

//        优化对小文件的处理——>设置读取数据的方式为：CombineTextInputFormat
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMinInputSplitSize(job,2097152);
        CombineTextInputFormat.setMaxInputSplitSize(job,4194304);

        job.setMapperClass(CombineInput.combineMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setReducerClass(CombineInput.combineReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        Path inPath = new Path("./datas/stuScore*");
        Path outPath = new Path("./TextInputFormat003_Combine");
        FileInputFormat.addInputPath(job,inPath);
        FileOutputFormat.setOutputPath(job,outPath);
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(outPath)){
            fs.delete(outPath,true);
        }

        boolean isCompletion = job.waitForCompletion(true);
        if(isCompletion){
            System.out.println("success......");
        }else{
            System.out.println("fail......");
        }
    }
}

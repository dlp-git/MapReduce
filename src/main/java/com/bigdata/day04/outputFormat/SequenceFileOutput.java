package com.bigdata.day04.outputFormat;

import com.bigdata.day04.inputFormat.CombineInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class SequenceFileOutput {
    public static class sequenceFileMap extends Mapper<LongWritable, Text,Text, DoubleWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] info = value.toString().split(",");
            if(info.length==3){
                context.write(new Text(info[0]),new DoubleWritable(Double.parseDouble(info[2])));
            }
        }
    }
    public static class sequenceFileReduce extends Reducer<Text, DoubleWritable,Text, DoubleWritable> {
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
        Job job=Job.getInstance(conf,"SequenceFileOutputFormat");
        job.setJarByClass(SequenceFileOutput.class);

//        优化对小文件的处理——>设置读取数据的方式为：CombineTextInputFormat
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMinInputSplitSize(job,2097152);
        CombineTextInputFormat.setMaxInputSplitSize(job,4194304);

//        修改输出文件的方式为：SequenceFileOutputFormat
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapperClass(SequenceFileOutput.sequenceFileMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setReducerClass(SequenceFileOutput.sequenceFileReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        Path inPath = new Path("./datas/stuScore*");
        Path outPath = new Path("./TextOutputFormat001_SequenceFile");
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

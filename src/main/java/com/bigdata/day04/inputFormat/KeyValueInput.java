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
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class KeyValueInput {
    /**
     * 默认TextInputFormat：一个block为一次切片，每次输入一个block
     * KeyValueTextInputFormat：每行由分隔符字节分为键和值部分
     *  B:A,C,E,K
     *  A:B,C,D,F,E,O  friends.txt
     *  需求： 求出共同好友，如： A-B C E    A和B的共同好友是C E
     *  ——>使用KeyValueTextInputFormat来读取数据,指定分割字节为 ":"
     *  第一步：找出具有相同好友的用户列表
     */
    public static class friendsMap extends Mapper<Text,Text,Text,Text>{
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] friends = value.toString().split(",");
            for (String friend : friends) {
                context.write(new Text(friend),key);
            }
        }
    }
    public static class friendsReduce extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer users = new StringBuffer();
            for (Text user : values) {
                users.append(user).append("\t");
            }
            String value = users.substring(0, users.lastIndexOf("\t"));
            context.write(key,new Text(value));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job=Job.getInstance(conf,"KeyValueTextInputFormat");
        job.setJarByClass(KeyValueInput.class);

//        设置读取数据的方式为：KeyValueTextInputFormat
        job.setInputFormatClass(KeyValueTextInputFormat.class);
//        修改KeyValueTextInputFormat默认分隔符（ ）为 “:”
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR,":");
        job.setMapperClass(friendsMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(friendsReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Path inPath = new Path("./datas/f*");
        Path outPath = new Path("./TextInputFormat002_KeyValue");
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

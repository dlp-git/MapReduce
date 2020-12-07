package com.bigdata.day01.mapReduceDemo;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

//    实现词频统计的MapReduce过程
public class wcMapReduce {
    /**
     * 1.实现map阶段：继承Mapper父类，重写父类map方法
     * KEYIN 偏移量（读取数据量）, VALUEIN 读取的数据（内容）,
     * KEYOUT 自定义的key类型, VALUEOUT 自定义的value类型
     */
    public static class wcMap extends Mapper<LongWritable,Text,Text,IntWritable> {
        @Override
    //    IntWritable key 偏移量, Text value 每行读取内容, Context context 提交任务
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    //        1.1.分割每行内容
            String[] words = value.toString().split(" ");
    //        1.2.为每个单词打标签,并提交数据
    //        序列化key和value
            Text wordText = new Text();
            IntWritable intWritable = new IntWritable(1);
            for (String word : words) {
                wordText.set(word);
                context.write(wordText,intWritable);
            }
        }
    }
    /**
     * 2.实现reduce阶段：继承Redecer父类，重写reduce方法
     *KEYIN 单词, VALUEIN 单词标签1, KEYOUT 聚合后的单词, VALUEOUT 聚合后的单词value值的和
     */
    public static class wcReduce extends Reducer<Text,IntWritable,Text,IntWritable> {
        @Override
//        Text key 单词（reduce端每次只接收一组相同的单词）,
//        Iterable<IntWritable> values 单词的value值，即一组 1,
//        Context context 提交任务
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//          2.1.将value值相加求和
            int sum=0;
            for (IntWritable value : values) {
//              获取IntWritable的value值
                sum+=value.get();
            }
//          2.2.提交数据
            context.write(key,new IntWritable(sum));
        }
    }
    /**
     * 书写main方法关联map类和reduce类
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//        MapReduce中通过job类对任务进行设定和提交
//      3.1.设定任务
        Configuration conf = new Configuration();
        Job job=Job.getInstance(conf,"wcMapReduce");
//      3.2.指定主类
        job.setJarByClass(wcMapReduce.class);
//      3.3.指定map类
        job.setMapperClass(wcMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
//      3.4.指定reduce类
        job.setReducerClass(wcReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
//      3.5.指定combiner类
        job.setCombinerClass(wcReduce.class);
//      3.6设置reduceTask的数量
        job.setNumReduceTasks(3);
//      3.7.指定被分析文件的路径
        Path inputPath = new Path("./datas/wcChinese.txt");
//        Path inputPath = new Path(args[0]);
        FileInputFormat.addInputPath(job,inputPath);
//      3.8.指定分析结果存放路径
        Path outputPath = new Path("./output001");
//        Path outputPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job,outputPath);
//        判断路径是否存在，若存在则删除路径
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(outputPath)){
            fs.delete(outputPath,true);
        }
//      3.9.提交任务
        boolean completion = job.waitForCompletion(true);
        if(completion){
            System.out.println("success......");
        }else{
            System.out.println("fail......");
        }
    }

}

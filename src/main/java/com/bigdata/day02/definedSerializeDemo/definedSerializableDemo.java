package com.bigdata.day02.definedSerializeDemo;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//实现自定义对象序列化的输出
public class definedSerializableDemo {
    public static class serializableMap extends Mapper<LongWritable, Text, NullWritable,stuScoreBean> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] stuInfo = value.toString().split(",");
            if(stuInfo.length==3){
                stuScoreBean ss=new stuScoreBean(stuInfo[0],stuInfo[1],Double.parseDouble(stuInfo[2]));
                context.write(NullWritable.get(),ss);
            }
        }
    }

}

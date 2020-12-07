package com.bigdata.day03.definedComparableDemo;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ScoreDefinedComparable {
    /**
     *  需求：对每门课程的学生的成绩进行排序 stuScore.txt
     *  map out:<subject,nullWritable>
     *  comparable :stu.score
     */
    public static class comparableMap extends Mapper<LongWritable, Text, stuComparaBean, NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] stuInfo = value.toString().split(",");
            if(stuInfo.length==3){
                stuComparaBean stuComparaBean = new stuComparaBean(stuInfo[0], stuInfo[1], Double.parseDouble(stuInfo[2]));
                context.write(stuComparaBean,NullWritable.get());
            }

        }
    }
}

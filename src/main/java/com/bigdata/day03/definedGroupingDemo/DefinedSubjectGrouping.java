package com.bigdata.day03.definedGroupingDemo;


import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DefinedSubjectGrouping {
    /**
     * 需求：统计每门课程的学员的前两名 stuScore.txt
     * map out: <stu,NullWritable>
     *  grouping: stu.subject
     *  reduce out:<Text,Text>
      */
    //map阶段
    public static class DefSubMap extends Mapper<LongWritable, Text, stuGroupBean, NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] stuInfo = value.toString().split(",");
            stuGroupBean stuBean = new stuGroupBean(stuInfo[0],stuInfo[1],Double.parseDouble(stuInfo[2]));
            context.write(stuBean, NullWritable.get());
        }
    }
    //自定义分组
    public static class DefSubGroup extends WritableComparator {
        public DefSubGroup(){
            super(stuGroupBean.class,true);
        }
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            stuGroupBean a1 = (stuGroupBean) a;
            stuGroupBean b1 = (stuGroupBean) b;
            return a1.getSubject().compareTo(b1.getSubject());
        }
    }
    //reduce阶段
    public static class DefSubReduce extends Reducer<stuGroupBean, NullWritable, stuGroupBean, NullWritable>{
        @Override
        protected void reduce(stuGroupBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            int i=1;
            for (NullWritable value : values) {
                if(i<3) {
                    context.write(key, NullWritable.get());
                    i++;
                }else{
                    break;
                }
            }
        }
    }

}

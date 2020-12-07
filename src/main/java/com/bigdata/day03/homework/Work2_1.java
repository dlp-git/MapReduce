package com.bigdata.day03.homework;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class Work2_1 {
    /**     B:A,C,E,K
     *      A:B,C,D,F,E,O  friends.txt
     * 需求： 求出共同好友，如： A-B C E    A和B的共同好友是C E
     * 第一步：找出具有相同好友的用户列表
     */
    public static class friendsMap extends Mapper<LongWritable, Text,Text, Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] info = value.toString().split(":");
            String[] friends = info[1].split(",");
            for (String friend : friends) {
                context.write(new Text(friend),new Text(info[0]));
                System.out.print(friend+"->"+info[0]+"|");
            }
            System.out.println();
        }
    }
    public static class friendsReduce extends Reducer<Text,Text,Text,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer users = new StringBuffer();
            for (Text user : values) {
                users.append(user.toString()+",");
            }
            String f = key.toString();
            String u = users.toString();
            context.write(new Text(f+" "+u.substring(0,u.lastIndexOf(","))),NullWritable.get());
        }
    }

}

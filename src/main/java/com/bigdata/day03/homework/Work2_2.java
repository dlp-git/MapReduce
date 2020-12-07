package com.bigdata.day03.homework;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.sql.Array;
import java.util.ArrayList;
import java.util.Arrays;

public class Work2_2 {
    /**     B:A,C,E,K
     *      A:B,C,D,F,E,O  friends.txt
     * 需求： 求出共同好友，如： A-B C E    A和B的共同好友是C E
     * 第二步：key：将用户列表两两组合；value：friend
     */
    public static class friendsMap extends Mapper<LongWritable, Text,Text, Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] info = value.toString().split(" ");
            String[] friends = info[1].split(",");
            Arrays.sort(friends);
                for (int i = 0; i < friends.length-1; i++) {
                    for (int j = i+1; j < friends.length; j++) {
                        String two = friends[i] + friends[j];
                        System.out.print(two+" ");
                            context.write(new Text(two),new Text(info[0]));
                    }
                }
            System.out.println();
        }
    }
    public static class friendsReduce extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer friends = new StringBuffer();
            for (Text user : values) {
                friends.append(user.toString());
            }
            context.write(key,new Text(friends.toString()));
        }
    }

}

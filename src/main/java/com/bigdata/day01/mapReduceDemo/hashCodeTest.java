package com.bigdata.day01.mapReduceDemo;

public class hashCodeTest {
    public static void main(String[] args) {
        String s1="乙利平";
        String s2="张不佩";
        String s3="下上";
        String s4="南北";
        String s5="东西";
        String s6="东";
        System.out.println(s1.hashCode() % 3+"\t\t"+s2.hashCode() % 3+"\t\t"+s3.hashCode() % 3+"\t\t"+s4.hashCode() % 3+"\t\t"+s5.hashCode() % 3+"\t\t"+s6.hashCode() % 3);
    }
}

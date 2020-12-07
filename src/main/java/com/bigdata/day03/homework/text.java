package com.bigdata.day03.homework;

public class text {
    public static void main(String[] args) {
       String s1="a";
       String s2="b";
        char[] chars1 = s1.toCharArray();
        char[] chars2 = s2.toCharArray();
        System.out.println(chars1.length);
        if(chars1[0]>chars2[0]){
            System.out.println(chars1[0]);
        }else {
            System.out.println(chars2[0]);
        }
    }
}

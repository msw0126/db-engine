package com.taodatarobot.engine.py4j.util;

public class Util {
    public static String join(Iterable<String> strings, String splitter) {
        StringBuffer sb = new StringBuffer();
        for(String s:strings){
            sb.append(s+splitter);
        }
        String tmp = sb.toString();
        return tmp.substring(0, tmp.length()-splitter.length());
    }
}

package com.taodatarobot.engine.py4j;

import com.taodatarobot.engine.py4j.hive.HiveUtil;
import org.apache.log4j.Logger;
import py4j.GatewayServer;

public class Entry {
    static Logger logger = Logger.getLogger(HiveUtil.class.getName());
    private HiveUtil hiveUtil = new HiveUtil();

    public HiveUtil getHiveUtil() {
        return hiveUtil;
    }

    public static void main(String[] args) {
        Entry entry = new Entry();
        GatewayServer server = new GatewayServer(entry);
        server.start();
    }
}

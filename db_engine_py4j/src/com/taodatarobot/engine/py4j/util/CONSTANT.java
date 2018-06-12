package com.taodatarobot.engine.py4j.util;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class CONSTANT {

    private static Config config = ConfigFactory.load("engine.properties");

    public static final String HDFS_URL = config.getString("HDFS_URL");

    public static final String HDFS_USER = config.getString("HDFS_USER");

    public static final String YARN_LOG_DIRECTORY = config.getString("YARN_LOG_DIRECTORY");

    public static final String HIVE_DRIVER = config.getString("HIVE_DRIVER");

    public static final String HIVE_URL = config.getString("HIVE_URL");

    public static final String HIVE_USER = config.getString("HIVE_USER");

    public static final String HIVE_PASSWORD = config.getString("HIVE_PASSWORD");

}

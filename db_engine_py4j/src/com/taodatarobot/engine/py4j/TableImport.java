package com.taodatarobot.engine.py4j;

import com.alibaba.fastjson.JSON;
import com.taodatarobot.engine.py4j.hdfs.HDFSUtil;
import com.taodatarobot.engine.py4j.hive.HiveUtil;
import com.taodatarobot.engine.py4j.hive.bean.FieldType;
import com.taodatarobot.engine.py4j.hive.bean.UploadToHiveConfig;
import com.taodatarobot.engine.py4j.util.CONSTANT;
import com.taodatarobot.engine.py4j.util.Util;
import org.apache.commons.io.FileUtils;

import java.io.BufferedReader;
import java.io.File;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class TableImport {

    public static void uploadToHive(String configPath) throws Exception{
        BufferedReader br = null;
        Statement statement = null;
        Connection conn = null;
        try{
            Class.forName(CONSTANT.HIVE_DRIVER).newInstance();
            conn = DriverManager.getConnection(CONSTANT.HIVE_URL, CONSTANT.HIVE_USER, CONSTANT.HIVE_PASSWORD);
            String jsonString = FileUtils.readFileToString(new File(configPath));
            // parse config string as java object
            UploadToHiveConfig config = JSON.parseObject(jsonString, UploadToHiveConfig.class);

            List<String> fieldSqlList = new ArrayList<>();
            for(FieldType fieldType : config.getField_type()){
                String name = fieldType.getName();
                String type = fieldType.getType();
                String sql = String.format("`%s` %s", name, type);
                fieldSqlList.add(sql);
            }

            String fieldSql = Util.join(fieldSqlList, ",");
            String dropTmpTableSql = String.format("drop table if exists %s", config.getGen_table());
            String createTmpTableSql = String.format("create table %s (%s) ROW FORMAT DELIMITED FIELDS TERMINATED BY \"%s\" tblproperties(\"skip.header.line.count\"=\"0\")",
                    config.getGen_table(),
                    fieldSql,
                    config.getDelimiter()
            );
            String hdfsPath = config.getUpload_hdfs_path();
//            String loadTableSql = String.format("LOAD DATA INPATH '%s' OVERWRITE INTO TABLE %s", hdfsPath, config.getGen_table());

            // execute sql to create table
            statement = conn.createStatement();
            statement.execute(dropTmpTableSql);
            statement.execute(createTmpTableSql);
            System.out.println(dropTmpTableSql);
            System.out.println(createTmpTableSql);

            ClassLoader classLoader = HiveUtil.class.getClassLoader();
            /**
             getResource()方法会去classpath下找这个文件，获取到url resource, 得到这个资源后，调用url.getFile获取到 文件 的绝对路径
             */
            URL url = classLoader.getResource(config.getCsv_file());

            String csv_path = url.getPath();
            System.out.println(csv_path);
            String loadTableSql = String.format("LOAD DATA  INPATH '%s' OVERWRITE INTO TABLE %s", hdfsPath, config.getGen_table());
            // upload file to hdfs
            HDFSUtil.uploadToHDFS(csv_path, config.getUpload_hdfs_path());
            // load into hive
//            statement.execute(loadTableSql);
            System.out.println(loadTableSql);
            // delete hdfs csv file
            HDFSUtil.delete(config.getUpload_hdfs_path());
        }finally {
            if(statement!=null) statement.close();
            if(conn!=null) conn.close();
        }
    }


    public static void main(String[] args) throws Exception {
        uploadToHive(getFilePath("bank_detail.json"));
        uploadToHive(getFilePath("bill_detail.json"));
        uploadToHive(getFilePath("browse_history.json"));
        uploadToHive(getFilePath("overdue.json"));
        uploadToHive(getFilePath("user_info.json"));
        uploadToHive(getFilePath("german_credit.json"));
    }

    private static String getFilePath(String path){
        ClassLoader classLoader = TableImport.class.getClassLoader();
        URL url = classLoader.getResource("data/"+path);
        return url.getFile();
    }
}

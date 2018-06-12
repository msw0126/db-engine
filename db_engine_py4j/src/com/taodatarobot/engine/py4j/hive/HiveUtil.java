package com.taodatarobot.engine.py4j.hive;

import com.alibaba.fastjson.JSON;
import com.sun.scenario.effect.impl.sw.sse.SSEBlend_SRC_OUTPeer;
import com.taodatarobot.engine.py4j.hdfs.HDFSUtil;
import com.taodatarobot.engine.py4j.hive.bean.UploadToHiveConfig;
import com.taodatarobot.engine.py4j.util.CONSTANT;
import com.taodatarobot.engine.py4j.hive.bean.FieldType;
import com.taodatarobot.engine.py4j.util.Util;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.sql.*;
import java.util.*;

public class HiveUtil {

    static Logger logger = Logger.getLogger(HiveUtil.class.getName());

    private static ConnectionBeanPool POOL_HIVE;
    private static final String CLUSTER_NAME = "T";

    static{
        POOL_HIVE = new ConnectionBeanPool(CONSTANT.HIVE_DRIVER, CONSTANT.HIVE_URL, CONSTANT.HIVE_USER, CONSTANT.HIVE_PASSWORD);
        try {
            POOL_HIVE.get(CLUSTER_NAME);
        }catch (Exception e){
            logger.error(String.format("can not connect to %s with user %s", CONSTANT.HIVE_URL, CONSTANT.HIVE_USER));
            logger.info("please check your configuration");
            System.exit(1);
        }

    }

    public LinkedHashMap<String, List<String>> viewTable(String tableName, int limit) throws SQLException {
        ConnectionBean connectionBean = POOL_HIVE.get(CLUSTER_NAME);
        Connection conn = connectionBean.getConnection();
        LinkedHashMap<String, List<String>> result = new LinkedHashMap<>();
        Statement statement = null;
        try {
            statement = conn.createStatement();
            statement.execute(String.format("select * from %s limit %d",tableName,limit));
            ResultSet resultSet = statement.getResultSet();
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnNum = metaData.getColumnCount();

            for(int i=0;i<columnNum;i++){
                String columnName = metaData.getColumnName(i+1);
                result.put(columnName, new ArrayList<>());
            }

            while(resultSet.next()){
                for(String column : result.keySet()){
                    String v = ""+resultSet.getObject(column);
                    result.get(column).add(v);
                }
            }
            LinkedHashMap<String, List<String>> resultFinal = new LinkedHashMap<>();
            for(String column:result.keySet()){
                String[] col = column.split("\\.");
                resultFinal.put(col[col.length-1], result.get(column));
            }
            return resultFinal;
        } catch (SQLException e) {
            throw e;
        }finally {
            if(statement!=null) statement.close();
            conn.close();
        }
    }


    public List<String> listTable() throws SQLException {
        ConnectionBean connectionBean = POOL_HIVE.get(CLUSTER_NAME);
        Connection conn = connectionBean.getConnection();
        List<String> tables = new ArrayList<>();
        Statement statement = null;
        try {
            statement = conn.createStatement();
            statement.execute("show tables");
            ResultSet resultSet = statement.getResultSet();
            while(resultSet.next()){
                String tableName = resultSet.getString(1);
                tables.add(tableName);
            }
            return tables;
        } catch (SQLException e) {
            throw e;
        }finally {
            if(statement!=null) statement.close();
            POOL_HIVE.release(CLUSTER_NAME, false);
        }
    }

    public List<String> queryTable(String table) throws SQLException {
        ConnectionBean connectionBean = POOL_HIVE.get(CLUSTER_NAME);
        Connection conn = connectionBean.getConnection();
        List<String> tables = new ArrayList<>();
        Statement statement = null;
        try {
            statement = conn.createStatement();
            String sql = String.format("show tables like '*%s*'", table);
            statement.execute(sql);
            ResultSet resultSet = statement.getResultSet();
            while(resultSet.next()){
                String tableName = resultSet.getString(1);
                tables.add(tableName);
            }
            return tables;
        } catch (SQLException e) {
            throw e;
        }finally {
            if(statement!=null) statement.close();
            POOL_HIVE.release(CLUSTER_NAME, false);
        }
    }

    public boolean checkExist(String table) throws SQLException {
        ConnectionBean connectionBean = POOL_HIVE.get(CLUSTER_NAME);
        Connection conn = connectionBean.getConnection();
        Statement statement = null;
        try {
            statement = conn.createStatement();
            String sql = String.format("show tables '%s'", table);
            statement.execute(sql);
            ResultSet resultSet = statement.getResultSet();
            return resultSet.next();
        } catch (SQLException e) {
            throw e;
        }finally {
            if(statement!=null) statement.close();
            POOL_HIVE.release(CLUSTER_NAME, false);
        }
    }

    public LinkedHashMap<String,String> describe(String table) throws SQLException {
        ConnectionBean connectionBean = POOL_HIVE.get(CLUSTER_NAME);
        Connection conn = connectionBean.getConnection();
        LinkedHashMap<String,String> tableDescribe = new LinkedHashMap<>();
        Statement statement = null;
        try {
            statement = conn.createStatement();
            String sql = String.format("describe %s", table);
            statement.execute(sql);
            ResultSet resultSet = statement.getResultSet();
            while(resultSet.next()){
                String field = resultSet.getString(1);
                if(field.equals("")) break;
                String fieldType = resultSet.getString(2);
                tableDescribe.put(field, fieldType);
            }
            return tableDescribe;
        } catch (SQLException e) {
            throw e;
        }finally {
            if(statement!=null) statement.close();
            POOL_HIVE.release(CLUSTER_NAME, false);
        }
    }

    public List<FieldType> describeAndSample(String table) throws SQLException {
        ConnectionBean connectionBean = POOL_HIVE.get(CLUSTER_NAME);
        ConnectionBean connectionBeanHive = POOL_HIVE.get(CLUSTER_NAME);
        Connection conn = connectionBean.getConnection();
        Connection connHive = connectionBeanHive.getConnection();
        List<FieldType> tableDescribe = new ArrayList<>();
        Statement statement = null;
        Statement statementHive = null;
        try {
            statementHive = connHive.createStatement();
            String sql = String.format("describe %s", table);
            statementHive.execute(sql);
            ResultSet resultSet = statementHive.getResultSet();
            StringBuilder fields = new StringBuilder();
            Map<String,Integer> indexMap = new HashMap<>();
            int i = 0;
            while(resultSet.next()){
                String field = resultSet.getString(1);
                if(field.equals("")) break;
                String fieldType = resultSet.getString(2);
                String type = fieldType.split("\\(")[0].toUpperCase();
                FieldType fType = new FieldType(field,type);
                tableDescribe.add(fType);
                if(type.equals("STRING")||type.equals("VARCHAR")||type.equals("CHAR")){
                    fields.append(String.format("`%s`,", field));
                    indexMap.put(field, i);
                }
                i++;
            }
            try {
                if (fields.length() != 0) {
                    statement = conn.createStatement();
                    statement.execute(String.format("select %s from %s limit %d", fields.substring(0, fields.length() - 1), table, 3));
                    resultSet = statement.getResultSet();
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    int columnNum = metaData.getColumnCount();

                    while (resultSet.next()) {
                        for (i = 0; i < columnNum; i++) {
                            String column = metaData.getColumnName(i + 1);
                            String v = resultSet.getObject(column) == null ? null : (resultSet.getObject(column) + "");
                            int index = indexMap.get(column);
                            tableDescribe.get(index).setSample(v);
                        }
                    }
                }
            }catch(SQLException e){}
            return tableDescribe;
        } catch (SQLException e) {
            throw e;
        }finally {
            if(statement!=null) statement.close();
            if(statementHive!=null) statementHive.close();
            conn.close();
            POOL_HIVE.release(CLUSTER_NAME, false);
        }
    }

    public void downLoadLog(String appId,String user, String dst) throws IOException{
        HDFSUtil.downLoadLog(appId, user, dst);
    }

    public void cleanComponent(String workingDir, String table) throws SQLException, IOException {
        HDFSUtil.deleteDirectory(workingDir);
        if(table!=null){
            dropTable(table);
        }
    }

    public void dropTable(String table) throws SQLException {
//        ConnectionBean connectionBean = POOL_IMPALA.create();
        ConnectionBean connectionBean = POOL_HIVE.get(CLUSTER_NAME);
        Connection conn = connectionBean.getConnection();
        Statement statement = null;
        try {
            statement = conn.createStatement();
            String sql = String.format("drop table if exists %s", table);
            statement.execute(sql);
        } catch (SQLException e) {
            throw e;
        }finally {
            if(statement != null) statement.close();
            conn.close();
        }
    }

    public void downloadFiles(List<String> from, List<String> to) throws IOException {


        System.out.println(from);
        System.out.println(to);
        int l = from.size();
        for(int i=0;i<l;i++){
            HDFSUtil.download(from.get(i), to.get(i));
            System.out.println("#################");
            System.out.println(from.get(i) + "------------>" + to.get(i));
        }
    }

    public void deleteHdfsFile(String hdfsPath) throws IOException {
        HDFSUtil.delete(hdfsPath);
    }



    public static List getFild(List<String> field){

        List mylist = new ArrayList();
        for(int i = 0 ; i < field.size() ; i++) {
            Object name = field.get(i);
            mylist.add(name);
        }
        return mylist;
    }

    public void uploadToHive(String tableName, String csvPath, String hdfsPath, String csvHdfsDir, List<String> field) throws Exception {
        BufferedReader br = null;
        Statement statement = null;
        try{
            ConnectionBean connectionBean = POOL_HIVE.get(CLUSTER_NAME);
            Connection conn = connectionBean.getConnection();

            String fieldSql = Util.join(getFild(field), ",");
            System.out.println(fieldSql);

            String dropTmpTableSql = String.format("drop table if exists %s", tableName);
            String createTmpTableSql = String.format("create external table taoshu_db_input.%s (%s) ROW FORMAT DELIMITED FIELDS TERMINATED BY \"%s\" location '%s' tblproperties(\"skip.header.line.count\"=\"0\")",
                    tableName,
                    fieldSql,
                    ",",
                    csvHdfsDir
            );
//            String loadTableSql = String.format("LOAD DATA INPATH '%s' OVERWRITE INTO TABLE robotxdata.%s", hdfsPath, tableName);
            // execute sql to create table

            statement = conn.createStatement();
            statement.execute(dropTmpTableSql);
            System.out.println(dropTmpTableSql);
            //upload file to hdfs
            HDFSUtil.uploadToHDFS(csvPath, hdfsPath);
            //创建外部表
            statement.execute(createTmpTableSql);
            System.out.println(createTmpTableSql);

        }finally {
            if(statement!=null) statement.close();
            POOL_HIVE.release(CLUSTER_NAME, false);
        }
    }
}

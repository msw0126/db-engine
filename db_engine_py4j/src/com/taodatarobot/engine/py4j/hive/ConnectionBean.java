package com.taodatarobot.engine.py4j.hive;

import java.sql.Connection;

public class ConnectionBean {
  
    private Connection connection; // 链接信息
    private long updateTime;       // 更新时间
  
    public Connection getConnection() {  
        return connection;  
    }  
  
    public void setConnection(Connection connection) {  
        this.connection = connection;  
    }  
  
    public long getUpdateTime() {  
        return updateTime;  
    }  
  
    public void setUpdateTime(long updateTime) {  
        this.updateTime = updateTime;  
    }  
}
package com.taodatarobot.engine.py4j.hive;

import java.sql.*;
import java.util.Date;

public class ConnectionBeanPool extends ObjectPool {
  
    private String url;  // 链接url
    private String usr;  // 账户名
    private String pwd;  // 密码  
  
    public ConnectionBeanPool(String driver, String url, String usr, String pwd) {  
        super();  
        try {  
            Class.forName(driver).newInstance();  
        } catch (Exception e) {  
            e.printStackTrace();  
        }  
        this.url = url;
        this.usr = usr;
        this.pwd = pwd;
    }  
  
    @Override
    public ConnectionBean create() {
        try {  
            ConnectionBean connectionBean = new ConnectionBean();  
            Connection connection = DriverManager.getConnection(url, usr, pwd);
            if (connection == null) {  
                System.out.print("null connection");  
            }  
            connectionBean.setConnection(connection);  
            connectionBean.setUpdateTime(new Date().getTime());
            return connectionBean;  
        } catch (SQLException e) {
            e.printStackTrace();  
            return null;  
        }  
    }  
  
    @Override  
    public void expire(ConnectionBean o) {  
        try {  
            o.getConnection().close();  
        } catch (SQLException e) {  
            e.printStackTrace();  
        }  
    }
  
    @Override  
    public boolean validate(ConnectionBean o) {  
        try {
            if(o.getConnection().isClosed()) return false;
            Statement stmt = o.getConnection().createStatement();
            stmt.execute("show tables LIKE SHOULD_BE_EMPTY");
            stmt.close();
            return true;
        } catch (SQLException e) {  
            e.printStackTrace();
            return false;  
        }  
    }
}
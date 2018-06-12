package com.taodatarobot.engine.py4j.hive;

import org.slf4j.Logger;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public abstract class ObjectPool {
  
    static Logger logger = org.slf4j.LoggerFactory.getLogger(ObjectPool.class);
    private static long expirationTime;  
  
    private static HashMap<String, ConcurrentHashMap<String, ConnectionBean>> locked;
    private static HashMap<String, ConcurrentLinkedQueue<ConnectionBean>> unlocked;
  
    public ObjectPool() {  
        if (locked == null) {  
            locked = new HashMap<>();
        }  
        if (unlocked == null) {
            unlocked = new HashMap<>();  
        }  
        expirationTime = 30 * 60 * 1000; // 30 minute 过期  
    }  
  
    protected abstract ConnectionBean create();  
  
    public abstract boolean validate(ConnectionBean o);  
  
    public abstract void expire(ConnectionBean o);  
  
    public ConnectionBean get(String clusterName) {
        synchronized (locked) {  
            String key = Thread.currentThread().getName() + clusterName;  
            logger.info("【POOL】 lock the LOCKED map, the clusterName is {}", clusterName);  
            long now = System.currentTimeMillis();  
            ConcurrentLinkedQueue<ConnectionBean> beans;  
            if (!unlocked.isEmpty()) {  
                beans = unlocked.get(clusterName);  
                if (beans != null) {  
                    while (!beans.isEmpty()) {  
                        // 获取头元素，并在资源队列中删除头元素  
                        ConnectionBean bean = beans.poll();  
                        // 如果头元素的时间过期了，那么关闭连接  
                        if (now - bean.getUpdateTime() > expirationTime) {  
                            logger.info("【POOL】 the connection is out of time ,bean time is {}", bean.getUpdateTime());  
                            // 释放  
                            expire(bean);  
                            bean = null;  
                        } else {  
                            if (validate(bean)) {  
                                logger.info("【POOL】 get the connection from poll and the clusterName is {}",  
                                        clusterName);  
                                bean.setUpdateTime(now);  
                                // 放入锁定的队列中并返回 锁定队列需要  
                                locked.get(clusterName).put(key, bean);  
                                return bean;  
                            } else {  
                                // 如果链接已经关闭  
                                unlocked.remove(clusterName);  
                                expire(bean);  
                                bean = null;  
                            }  
                        }  
                    }  
                }  
            }  
            // 由于unlock可能为空，所以初始化对应的clusterName  
            unlocked.put(clusterName, new ConcurrentLinkedQueue<ConnectionBean>());  
            // 如果没有链接则新建一个操作  
            ConnectionBean bean = create();  
            logger.info("【POOL】 the pool could not provide a connection, so create it,clusterName is {}",  
                    clusterName);  
            if (locked.get(clusterName) == null) {  
                logger.info("【POOL】 the clusterName in pool is null, create a new Map in LOCKED, clusterName is {}",  
                        clusterName);  
                locked.put(clusterName, new ConcurrentHashMap<String, ConnectionBean>());  
            }  
            locked.get(clusterName).put(key, bean);  
            return bean;  
  
        }  
    }  
  
  
    public void release(String clusterName, boolean close) {
        synchronized (locked) {  
            String key = Thread.currentThread().getName() + clusterName;  
            ConcurrentHashMap<String, ConnectionBean> connectionBeans = locked.get(clusterName);  
            ConnectionBean bean = connectionBeans.get(key);  
            connectionBeans.remove(key);  
            bean.setUpdateTime(System.currentTimeMillis());
            if(close)
                try {
                    bean.getConnection().close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            unlocked.get(clusterName).add(bean);  
        }
    }  
}  
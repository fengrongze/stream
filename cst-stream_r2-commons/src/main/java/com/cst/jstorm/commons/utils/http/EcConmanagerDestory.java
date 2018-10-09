package com.cst.jstorm.commons.utils.http;

import org.apache.http.conn.HttpClientConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import java.util.concurrent.TimeUnit;

/**
 * @author Johnney.chiu
 * create on 2017/11/29 18:18
 * @Description 处理无效链接
 */
public class EcConmanagerDestory  extends Thread {

    private final static Logger logger = LoggerFactory.getLogger(EcConmanagerDestory.class);

    private final HttpClientConnectionManager connMgr;

    @Value(value ="${http.maxIdleTime}")
    private int maxIdleTime;

    private volatile boolean shutdown;

    public EcConmanagerDestory(HttpClientConnectionManager connMgr) {
        this.connMgr = connMgr;
        // 启动当前线程
        this.start();
    }

    @Override
    public void run() {
        try {
            while (!shutdown) {
                synchronized (this) {
                    wait(maxIdleTime*1000);
                    if (connMgr == null) {
                        Thread.interrupted();
                    }
                    // 关闭失效的连接
                    connMgr.closeExpiredConnections();
                    connMgr.closeIdleConnections(30, TimeUnit.SECONDS);

                }
            }
        } catch (InterruptedException ex) {
            // 结束
            logger.error("interrupted excpetion:{}",ex);
        }
    }

    public void shutdown() {
        shutdown = true;
        synchronized (this) {
            notifyAll();
        }
    }

    public int getMaxIdleTime() {
        return maxIdleTime;
    }

    public void setMaxIdleTime(int maxIdleTime) {
        this.maxIdleTime = maxIdleTime;
    }
}

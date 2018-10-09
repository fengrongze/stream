package com.cst.jstorm.commons.utils.http;

import org.apache.http.client.config.RequestConfig;
import org.springframework.beans.factory.annotation.Value;

/**
 * @author Johnney.chiu
 * create on 2017/11/30 11:22
 * @Description request设置
 */
public class RequestConfigBuilder {

    //创建连接的最长时间
    @Value("${http.connectTimeout}")
    private int connectTimeout;

    //从连接池中获取到连接的最长时间
    @Value("${http.connectionRequestTimeout}")
    private int connectionRequestTimeout;

    //socketTimeout
    @Value("${http.socketTimeout}")
    private int socketTimeout;


    private RequestConfig.Builder requestCOnfigBuilder = null;

    public  void  init(){
        requestCOnfigBuilder = RequestConfig.custom().setConnectionRequestTimeout(this.connectionRequestTimeout)
                .setConnectTimeout(this.connectTimeout).setSocketTimeout(this.socketTimeout);
    }

    public RequestConfig getRequestConfig(){
        return requestCOnfigBuilder.build();
    }

    public int getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public int getConnectionRequestTimeout() {
        return connectionRequestTimeout;
    }

    public void setConnectionRequestTimeout(int connectionRequestTimeout) {
        this.connectionRequestTimeout = connectionRequestTimeout;
    }

    public int getSocketTimeout() {
        return socketTimeout;
    }

    public void setSocketTimeout(int socketTimeout) {
        this.socketTimeout = socketTimeout;
    }
}

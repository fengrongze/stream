package com.cst.jstorm.commons.utils.spring.configuration;

import com.cst.jstorm.commons.utils.http.EcConmanagerDestory;
import com.cst.jstorm.commons.utils.http.HttpConnectionManager;
import com.cst.jstorm.commons.utils.http.HttpUtils;
import com.cst.jstorm.commons.utils.http.RequestConfigBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Johnney.Chiu
 * create on 2018/2/5 15:22
 * @Description
 * @title
 */
@Configuration
public class BasicSyncHttpclientConfiguration {


    @Bean(name = "httpClientConnectionManager",initMethod = "init")
    public HttpConnectionManager createHttpConnectionManager(){
        return new HttpConnectionManager();
    }

    @Bean(destroyMethod = "shutdown")
    public EcConmanagerDestory createEcConmanagerDestory(HttpConnectionManager httpConnectionManager){
        return new EcConmanagerDestory(httpConnectionManager.getCm());
    }
    @Bean(name="requestConfigBuilder",initMethod = "init")
    public RequestConfigBuilder createRequestConfigBuilder(){
        return new RequestConfigBuilder();
    }

    @Bean(name="httpUtils")
    public HttpUtils createHttpUtils(HttpConnectionManager httpConnectionManager, RequestConfigBuilder requestConfigBuilder){
        return new HttpUtils(httpConnectionManager.getHttpClient(), requestConfigBuilder.getRequestConfig());
    }

}

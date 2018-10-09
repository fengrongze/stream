package com.cst.bigdata.config;

import com.cst.base.factory.GdcpFactory;
import com.cst.bigdata.config.props.BigDaoProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Johnney.Chiu
 * create on 2018/6/22 15:18
 * @Description bigdao config
 * @title
 */
@Configuration
@EnableConfigurationProperties(value = {BigDaoProperties.class})
public class BigDaoConfig {

    @Autowired
    private BigDaoProperties bigDaoProperties;


    @Bean(name="gdcpFactory")
    public GdcpFactory getGdcpFactoryProxy(){
       return GdcpFactory.init(bigDaoProperties.getGdcpFactoryClientSystem(),
               bigDaoProperties.getGdcpFactoryClientProxyUrl());
    }

}

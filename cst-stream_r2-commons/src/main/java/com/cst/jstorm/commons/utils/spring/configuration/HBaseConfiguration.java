package com.cst.jstorm.commons.utils.spring.configuration;

import com.cst.jstorm.commons.utils.HBaseConnectionUtil;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

/**
 * @author Johnney.Chiu
 * create on 2018/2/5 15:17
 * @Description
 * @title
 */
@Configuration
public class HBaseConfiguration {
    public final static Logger logger = LoggerFactory.getLogger(HBaseConfiguration.class);

    @Bean(name="hbaseConnectionProperties")
    public HBaseConnectionUtil.HbaseConnectionProperties getHBaseConnectionProperties(){
        return new HBaseConnectionUtil.HbaseConnectionProperties();
    }

    @Bean(name="hBaseConnection")
    public Connection getHBaseConnection(HBaseConnectionUtil.HbaseConnectionProperties hbaseConnectionProperties){
        try {
            logger.debug(hbaseConnectionProperties.toString());
            return HBaseConnectionUtil.createHBaseConnnection(hbaseConnectionProperties);
        } catch (IOException e) {
            logger.error("get hbase {} connection error:{}",hbaseConnectionProperties.toString(),e);
            e.printStackTrace();
        }
        return null;
    }
}

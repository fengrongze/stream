package com.cst.bigdata.config;

import com.cst.bigdata.config.props.HbaseProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

import java.io.IOException;

/**
 * @author Johnney.Chiu
 * create on 2018/9/17 14:52
 * @Description hbase connection
 * @title
 */
@Configuration
@Slf4j
@EnableConfigurationProperties(value = {HbaseProperties.class})
public class HbaseConnectionConfig {

    private static final String HBASE_QUORUM = "hbase.zookeeper.quorum";
    private static final String HBASE_ROOTDIR = "hbase.rootdir";
    private static final String HBASE_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort";
    private static final String HBASE_ZNODE_PARENT = "zookeeper.znode.parent";


    @Autowired
    private HbaseProperties hbaseProperties;

    @Bean("otherHbaseConnection")
    public Connection createHBaseConnnection()  {
        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        configuration.set(HBASE_QUORUM, hbaseProperties.getZkQuorum());
        configuration.set(HBASE_ROOTDIR, hbaseProperties.getRootDir());
        //configuration.set(HBASE_ZNODE_DATA, zkDataDir);
        configuration.set(HBASE_ZOOKEEPER_CLIENTPORT, hbaseProperties.getZkPort());
        if(!StringUtils.isEmpty(hbaseProperties.getZnodeParent()))
            configuration.set(HBASE_ZNODE_PARENT, hbaseProperties.getZnodeParent());
        try {
            return ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            log.error("create connection error",e);
        }
        return null;
    }

}

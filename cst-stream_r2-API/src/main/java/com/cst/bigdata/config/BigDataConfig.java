package com.cst.bigdata.config;

import com.cst.bigdata.config.props.HadoopProperties;
import com.cst.bigdata.config.props.HbaseProperties;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.util.StringUtils;

/**
 * @author Johnney.chiu
 * create on 2017/11/23 13:07
 * @Description 大数据配置
 */
@Configuration
@EnableConfigurationProperties(value = {HbaseProperties.class, HadoopProperties.class})
@ConditionalOnClass(HbaseTemplate.class)
public class BigDataConfig {

    private static final String HBASE_QUORUM = "hbase.zookeeper.quorum";
    private static final String HBASE_ROOTDIR = "hbase.rootdir";
    private static final String HBASE_ZNODE_DATA = "hbase.zookeeper.property.dataDir";
    private static final String HBASE_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort";
    private static final String HBASE_ZOOKEEPER_ZNODE_PARENT = "zookeeper.znode.parent";
    private static final String HADOOP_HOME_DIR = "hadoop.home.dir";
    private static final String CROSS_PLATFORM = "mapreduce.app-submission.cross-platform";
    private static final String UBERTASK = "mapreduce.job.ubertask.enable";



    @Autowired
    private HbaseProperties hbaseProperties;


    @Autowired
    HadoopProperties hadoopProperties;

    @Bean(name="haddopConfiguration")
    public org.apache.hadoop.conf.Configuration createConfiguration(){
        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        /*System.setProperty(HADOOP_HOME_DIR, hadoopProperties.getHadoopHomeDir());
        hadoopProperties.getHadoopCoreFile().forEach(item -> configuration.addResource(item));
        configuration.set(CROSS_PLATFORM, hadoopProperties.getCrossPlatform());
        configuration.set(UBERTASK, hadoopProperties.getUbertask());*/
        //configuration.setUser("user1");
        //configuration.set("mapreduce.job.jar", "E:\\github\\hadoop\\target\\fulei-1.0-SNAPSHOT.jar");
        // configuration.set("HADOOP_USER_NAME","user1");
        return configuration;
    }


    @Bean
    @ConditionalOnMissingBean(HbaseTemplate.class)
    public HbaseTemplate hbaseTemplate(org.apache.hadoop.conf.Configuration haddopConfiguration) {
        haddopConfiguration.set(HBASE_QUORUM, hbaseProperties.getZkQuorum());
        //haddopConfiguration.set(HBASE_ROOTDIR, hbaseProperties.getRootDir());
        //haddopConfiguration.set(HBASE_ZNODE_DATA, hbaseProperties.getZkDataDir());
        haddopConfiguration.set(HBASE_ZOOKEEPER_CLIENTPORT,hbaseProperties.getZkPort());
        if(!StringUtils.isEmpty(hbaseProperties.getZnodeParent()))
            haddopConfiguration.set(HBASE_ZOOKEEPER_ZNODE_PARENT, hbaseProperties.getZnodeParent());
        HbaseTemplate hbaseTemplate = new HbaseTemplate(haddopConfiguration);

        return hbaseTemplate;
    }

}

package com.cst.jstorm.commons.utils;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import com.cst.config.client.ConfigClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @author Johnney.chiu
 * create on 2017/11/17 15:54
 * @Description 定义加载Topology的数据
 */
public abstract class AbstractTopologyInitialization {

    private static final Logger logger = LoggerFactory.getLogger(AbstractTopologyInitialization.class);
    /**
     * 通过config.properties的配置参数获取zookeeper的参数配置
     * @return
     */
    protected Properties loadProp(String localConfigName) {
        Properties prop = getLocalProperties(localConfigName);
        ConfigClient config = new ConfigClient();
//        config.setEnableUsing(false);
        enrichConfigClient(config, prop);
        config.start();
        config.loadProperties(prop);
        config.stop();
        return prop;
    }

    /**
     * 通过配置文件名称获取到相应的本地基本配置
     * @param localConfigName
     * @return
     */
    public  Properties getLocalProperties(String localConfigName){
        Properties properties = new Properties();
        try {
            properties.load(PropertiesUtil.class.getClassLoader().getResourceAsStream(localConfigName));
        } catch (Exception e) {
            logger.error("properties load failure", e);
        }
        return properties;
    }

    /**
     * 给config设置属性，方便改写
     * @param config
     * @param properties
     */
    public void enrichConfigClient(ConfigClient config,Properties properties){
        config.setZookeeperUrl(properties.getProperty("zookeeper.url"));
        config.setLevels(properties.getProperty("levels"));
        config.setConfigFilename(properties.getProperty("config.filename"));
    }


    /**
     * 通过property属性对象，向config写一些键值对
     * @param props
     * @return
     */
    public Config initConf(Properties props,String param) {
        Config conf = new Config();
        return conf;
    }



    /**
     * 拓扑的创建
     *  @param props
     * @return
     */
    protected  abstract TopologyBuilder createBuilder(Properties props,String ... param) ;
    protected String getName(String pre,String param){
        if(param.contains("obd"))
            return pre + "_obd";
        if(param.contains("gps"))
            return pre + "_gps";
        if(param.contains("integrated"))
            return pre + "_integrated";
        return pre;
    }

}

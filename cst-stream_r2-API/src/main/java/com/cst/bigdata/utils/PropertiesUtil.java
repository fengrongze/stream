package com.cst.bigdata.utils;

import com.cst.config.client.ConfigClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * 
 * cst.jstorm.commons.utils.PropertiesUtil
 * 2017年11月7日 上午10:24:22
 * @author lith
 * <p>desc:properties工具类</p>
 */
public class PropertiesUtil {

	private static final Logger logger = LoggerFactory.getLogger(PropertiesUtil.class);



	/**
	 * 通过config.properties的配置参数获取zookeeper的参数配置
	 * @return
	 */
	public static Properties loadProp() {
		Properties properties = new Properties();
		try {
			properties.load(PropertiesUtil.class.getClassLoader().getResourceAsStream("config.properties"));
			ConfigClient config=new ConfigClient();
			//config.setEnableUsing(false);
			config.setZookeeperUrl(properties.getProperty("zookeeper.url"));
			config.setLevels(properties.getProperty("levels"));
			config.setConfigFilename(properties.getProperty("config.filename"));
			config.start();
			config.loadProperties(properties);
			config.stop();
		} catch (Exception e) {
			logger.error("properties load failure", e);
		}
		return properties;
	}

	public static Properties initProp(Properties prop, boolean forceload) {
		if (prop == null||forceload) {
			logger.debug("props is null");
			return loadProp();
		}
		logger.debug("props is not null");
		return prop;
	}
	
}

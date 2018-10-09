package com.cst.bigdata.config.props;

import com.cst.bigdata.utils.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.Properties;

/**
 * @author Johnney.chiu
 * create on 2018/1/26 16:14
 * @Description  配置的管理
 */


public class EnvProperties extends PropertyPlaceholderConfigurer {

    private final static Logger logger = LoggerFactory.getLogger(EnvProperties.class);

    private Properties outerProps;
    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
        try {
            Properties mergedProps = mergeProperties();
            outerProps = PropertiesUtil.loadProp();
            logger.debug(outerProps.toString());
            mergedProps(outerProps,mergedProps);
            processProperties(beanFactory, mergedProps);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    public static void mergedProps(Properties myPros, Properties mergedProps) {
        CollectionUtils.mergePropertiesIntoMap(myPros, mergedProps);
    }


    public Properties getOuterProps() {
        return outerProps;
    }

    public void setOuterProps(Properties outerProps) {
        this.outerProps = outerProps;
    }


}



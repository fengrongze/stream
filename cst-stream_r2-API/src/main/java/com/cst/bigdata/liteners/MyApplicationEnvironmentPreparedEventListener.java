package com.cst.bigdata.liteners;

import com.cst.bigdata.utils.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationEnvironmentPreparedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.core.env.PropertySource;

import java.util.Iterator;
import java.util.Properties;

/**
 * @author Johnney.Chiu
 * create on 2018/2/9 16:55
 * @Description spring boot 配置环境事件监听
 * @title
 */
public class MyApplicationEnvironmentPreparedEventListener implements ApplicationListener<ApplicationEnvironmentPreparedEvent> {

    private final static Logger logger = LoggerFactory.getLogger(MyApplicationEnvironmentPreparedEventListener.class);

    @Override
    public void onApplicationEvent(ApplicationEnvironmentPreparedEvent applicationEnvironmentPreparedEvent) {
        logger.debug("application prepared environment ,Happy to u!");
        ConfigurableEnvironment environment = applicationEnvironmentPreparedEvent.getEnvironment();
        environment.getPropertySources().forEach(
                ps->{logger.debug("ps.name:{},ps.source:{},ps.class:{}",ps.getName(),ps.getSource(),ps.getClass());
                logger.debug("ps data:{}",ps.toString());});
        Properties outerProps = PropertiesUtil.loadProp();
        environment.getPropertySources().addFirst(new PropertiesPropertySource("cst",outerProps));


    }
}

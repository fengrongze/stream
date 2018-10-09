package com.cst.bigdata.liteners;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationPreparedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * @author Johnney.Chiu
 * create on 2018/2/11 10:27
 * @Description 上下文创建完成后执行的事件监听器
 * @title
 */
public class MyApplicationPreparedEventListener implements ApplicationListener<ApplicationPreparedEvent>{
    private static final Logger logger = LoggerFactory.getLogger(MyApplicationPreparedEventListener.class);

    @Override
    public void onApplicationEvent(ApplicationPreparedEvent applicationPreparedEvent) {
        ConfigurableApplicationContext cac = applicationPreparedEvent.getApplicationContext();
        logger.debug("configurable application context:id {},\n bean factory {},\nenvironment {}",
                cac.getId(),cac.getBeanFactory().toString(),cac.getEnvironment().toString());
    }
}

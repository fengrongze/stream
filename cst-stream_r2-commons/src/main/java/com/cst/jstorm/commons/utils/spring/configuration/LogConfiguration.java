package com.cst.jstorm.commons.utils.spring.configuration;

import com.cst.jstorm.commons.utils.logger.LoggerReset;
import com.cst.jstorm.commons.utils.logger.SpringLoggerReSet;

/**
 * @author Johnney.Chiu
 * create on 2018/2/5 15:16
 * @Description
 * @title
 */
/*@Configuration*/
public class LogConfiguration {
    /*@Bean(name="logRset")*/
    public LoggerReset restLog(){
        return new SpringLoggerReSet();
    }
}

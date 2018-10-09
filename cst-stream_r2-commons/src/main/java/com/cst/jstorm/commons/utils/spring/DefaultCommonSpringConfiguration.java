package com.cst.jstorm.commons.utils.spring;

import com.cst.jstorm.commons.utils.spring.configuration.BasicSyncHttpclientConfiguration;
import com.cst.jstorm.commons.utils.spring.configuration.EnvConfiguration;
import com.cst.jstorm.commons.utils.spring.configuration.HBaseConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;

/**
 * @author Johnney.chiu
 * create on 2017/11/30 10:48
 * @Description 我的配置类
 */
@Configuration
@ComponentScan(basePackages = {"com.cst.jstorm.commons.utils"})
@PropertySource(value = {"classpath:config.properties"})
@Import(value = {EnvConfiguration.class,/* LogConfiguration.class,*/ HBaseConfiguration.class,
        BasicSyncHttpclientConfiguration.class})
public class DefaultCommonSpringConfiguration {


}

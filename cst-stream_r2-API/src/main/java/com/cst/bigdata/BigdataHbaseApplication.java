package com.cst.bigdata;

import com.cst.bigdata.config.props.MybatisProperties;
import com.cst.bigdata.liteners.MyApplicationEnvironmentPreparedEventListener;
import com.cst.bigdata.liteners.MyApplicationPreparedEventListener;
import com.cst.bigdata.utils.ProfileUtil;
import org.mybatis.spring.annotation.MapperScan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationEnvironmentPreparedEvent;
import org.springframework.boot.context.event.ApplicationFailedEvent;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.boot.context.event.ApplicationStartingEvent;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

/**
 * @author Johnney.chiu
 * create on 2017/11/21 17:01
 * @Description 用于大数据计算的hbase读写服务
 */

@SpringBootApplication
@EnableTransactionManagement()
@EnableWebMvc
@MapperScan(basePackages = "com.cst.bigdata.repository.mybatis")
public class BigdataHbaseApplication {

    private final static Logger logger = LoggerFactory.getLogger(BigdataHbaseApplication.class);

    public static void main(String... args){

       //ConfigurableApplicationContext context= SpringApplication.run(BigdataHbaseApplication.class, args);

        SpringApplication springApplication = new SpringApplication(BigdataHbaseApplication.class);

        ProfileUtil.addDefaultProfile(springApplication);

        //some  listener
        springApplication.addListeners((ApplicationEvent ae)->logger.debug("application event:{}",ae));

        springApplication.addListeners((ApplicationStartingEvent ase)->logger.debug("application starting event{}",ase));
        springApplication.addListeners(
                (ApplicationEnvironmentPreparedEvent aepe) -> logger.debug("application environment prepare event:{}",aepe));
        springApplication.addListeners((ApplicationFailedEvent afe)->logger.debug("application failed event:{}",afe));
        springApplication.addListeners(new MyApplicationEnvironmentPreparedEventListener());
        springApplication.addListeners(new MyApplicationPreparedEventListener());

        //run this application
        ConfigurableApplicationContext context = springApplication.run(args);

        Environment env = context.getEnvironment();
        logger.debug("application name:{},server port:{}",env.getProperty("spring.application.name"),env.getProperty("server.port"));

        logger.debug("sqlSessionFactory:,{}",context.getBean("sqlSessionFactory"));
        logger.debug("appOilPriceMapper,{}",context.getBean("appOilPriceMapper"));

    }
}

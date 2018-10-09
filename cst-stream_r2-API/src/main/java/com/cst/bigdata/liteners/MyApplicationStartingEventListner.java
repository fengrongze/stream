package com.cst.bigdata.liteners;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.event.ApplicationStartingEvent;
import org.springframework.context.ApplicationListener;

import java.util.Arrays;

/**
 * @author Johnney.Chiu
 * create on 2018/2/9 16:41
 * @Description 启动程序的监听
 * @title
 */
public class MyApplicationStartingEventListner implements ApplicationListener<ApplicationStartingEvent> {
    private final static Logger logger = LoggerFactory.getLogger(MyApplicationStartingEventListner.class);
    @Override
    public void onApplicationEvent(ApplicationStartingEvent applicationStartingEvent) {
        logger.debug("starting application ,Happy to u!");
        SpringApplication app = applicationStartingEvent.getSpringApplication();
        String[] args=applicationStartingEvent.getArgs();
        //Arrays.stream(args).anyMatch(arg -> "no_banner".equals(arg));
        Arrays.stream(args).forEach(a -> logger.debug(a));
    }
}

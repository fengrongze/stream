package com.cst.jstorm.commons.utils;

import com.cst.jstorm.commons.utils.logger.LoggerReset;
import com.cst.jstorm.commons.utils.logger.OtherLoggerReset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * @author Johnney.Chiu
 * create on 2018/3/9 16:07
 * @Description 不使用spring的方式更改loggerContext
 * @title
 */
public class LogbackInitUtil {
    private static final Logger logger = LoggerFactory.getLogger(LogbackInitUtil.class);

    public static void changeLogback(Properties properties,boolean flag){
        if(flag)
            return;
        LoggerReset loggerReset = new OtherLoggerReset(properties);

        try {
            loggerReset.doConfiLogBack();
        } catch (IOException e) {
            logger.error("init log error is :{}",e);
        }
    }
}

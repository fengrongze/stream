package com.cst.jstorm.commons.utils.logger;

import ch.qos.logback.classic.LoggerContext;
import com.alibaba.jstorm.utils.JStormUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @author Johnney.Chiu
 * create on 2018/2/1 15:50
 * @Description
 * @title
 */
public class OtherLoggerReset extends LoggerReset {

    private final static Logger logger = LoggerFactory.getLogger(OtherLoggerReset.class);
    private Properties properties;

    public OtherLoggerReset(Properties properties) {
        this.properties = properties;
    }

    @Override
    public void richContext(LoggerContext loggerContext) {

        String logFile = properties.getProperty("spring.log.settingFile");
        if(!StringUtils.isEmpty(logFile))
            super.setLogFile(logFile);

        loggerContext.putProperty("app.name",properties.getProperty("app.name"));
        loggerContext.putProperty("logger.root", properties.getProperty("logger.root"));
        loggerContext.putProperty("logback.env", properties.getProperty("logback.env"));

        loggerContext.putProperty("logback.depart", properties.getProperty("logback.depart"));
        loggerContext.putProperty("log.level", properties.getProperty("log.level"));

        String systemLogPath=System.getProperty("jstorm.log.dir");
        if(org.springframework.util.StringUtils.isEmpty(systemLogPath))
            systemLogPath=JStormUtils.getLogDir();
        loggerContext.putProperty("jstorm.log.dir",systemLogPath);
        loggerContext.putProperty("topology.name", properties.getProperty("storm.topology.name"));
        /*loggerContext.putProperty("logfile.name", JStormUtils.genLogName(properties.getProperty("storm.topology.name"),
                Integer.parseInt(properties.getProperty("jstorm.port"))));
        */
        loggerContext.putProperty("logfile.name", JStormUtils.getLogFileName());
    }
}

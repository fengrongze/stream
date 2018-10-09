package com.cst.jstorm.commons.utils.logger;

import ch.qos.logback.classic.LoggerContext;
import com.alibaba.jstorm.utils.JStormUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Johnney.Chiu
 * create on 2018/2/1 15:21
 * @Description
 * @title
 */
public class SpringLoggerReSet extends LoggerReset implements ApplicationListener<ContextRefreshedEvent> {

    private final static Logger logger = LoggerFactory.getLogger(SpringLoggerReSet.class);

    @Value("${app.name}")
    private String appName;

    @Value("${logger.root}")
    private String loggerRoot;

    @Value("${logback.env}")
    private String logBackEnv;

    @Value("${logback.depart}")
    private String logBackDePart;

    @Value("${log.level}")
    private String loglevel;

    @Value("${spring.log.settingFile}")
    private String logFile;

    @Value("${jstorm.port}")
    private String jstormPort;

    @Value("${storm.topology.name}")
    private String topologyName;


    public SpringLoggerReSet(String logFile) {
            super(logFile);
    }

    public SpringLoggerReSet() {

    }

    @Override
    public void  richContext(LoggerContext loggerContext) {
        if(!StringUtils.isEmpty(logFile))
           super.setLogFile(logFile);
        Map<String, String> map = new HashMap<String,String>(){{
            put("app.name", appName);
            put("logger.root", loggerRoot);
            put("logback.env", logBackEnv);
            put("logback.depart", logBackDePart);
            put("log.level", loglevel);
            put("topology.name", topologyName);
            put("logfile.name", JStormUtils.genLogName(topologyName,Integer.parseInt(jstormPort)));
        }};

        String systemLogPath=System.getProperty("jstorm.log.dir");
        if(StringUtils.isEmpty(systemLogPath))
            systemLogPath=JStormUtils.getLogDir();

        map.put("jstorm.log.dir", systemLogPath);

        loggerContext.putProperty("jstorm.log.dir",systemLogPath);
            for (Map.Entry<String, String> entry : map.entrySet()) {
                if (entry.getValue().equals(loggerContext.getProperty(entry.getKey())))
                    continue;
                loggerContext.putProperty(entry.getKey(), entry.getValue());
            }

    }


    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        try {
            doConfiLogBack();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getLoggerRoot() {
        return loggerRoot;
    }

    public void setLoggerRoot(String loggerRoot) {
        this.loggerRoot = loggerRoot;
    }

    public String getLogBackEnv() {
        return logBackEnv;
    }

    public void setLogBackEnv(String logBackEnv) {
        this.logBackEnv = logBackEnv;
    }

    public String getLogBackDePart() {
        return logBackDePart;
    }

    public void setLogBackDePart(String logBackDePart) {
        this.logBackDePart = logBackDePart;
    }

    public String getLoglevel() {
        return loglevel;
    }

    public void setLoglevel(String loglevel) {
        this.loglevel = loglevel;
    }

    public String getJstormPort() {
        return jstormPort;
    }

    public void setJstormPort(String jstormPort) {
        this.jstormPort = jstormPort;
    }

    public String getTopologyName() {
        return topologyName;
    }

    public void setTopologyName(String topologyName) {
        this.topologyName = topologyName;
    }
}

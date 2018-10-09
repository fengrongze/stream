package com.cst.jstorm.commons.utils.logger;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;

/**
 * @author Johnney.Chiu
 * create on 2018/2/1 14:46
 * @Description 手动配置Logback
 * @title
 */
public abstract class LoggerReset {

    private static final Logger logger = LoggerFactory.getLogger(LoggerReset.class);

    private String logFile="logback.xml";



    public void doConfiLogBack() throws IOException {
        synchronized (this) {
            LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
            try {
                JoranConfigurator joranConfigurator = new JoranConfigurator();
                joranConfigurator.setContext(loggerContext);
                loggerContext.reset(); // override default configuration

                richContext(loggerContext);
                logger.info("logFile is :{}", logFile);
                joranConfigurator.doConfigure(new ClassPathResource(logFile).getInputStream());
            } catch (JoranException je) {
                // StatusPrinter will handle this
                je.printStackTrace();
            }
            StatusPrinter.printInCaseOfErrorsOrWarnings(loggerContext);
        }
    }

    //从状态中导入
    public abstract void richContext(LoggerContext loggerContext);



    public LoggerReset(String logFile) {
        this.logFile = logFile;
    }

    public LoggerReset() {
    }

    public String getLogFile() {
        return logFile;
    }

    public void setLogFile(String logFile) {
        this.logFile = logFile;
    }
}

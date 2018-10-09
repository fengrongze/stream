package com.cst.bigdata.config;

import com.cst.bigdata.config.props.FileProperties;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.embedded.EmbeddedServletContainerFactory;
import org.springframework.boot.context.embedded.jetty.JettyEmbeddedServletContainerFactory;
import org.springframework.boot.context.embedded.jetty.JettyServerCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.servlet.MultipartConfigFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import javax.servlet.MultipartConfigElement;

/**
 * @author Johnney.chiu
 * create on 2017/12/8 18:10
 * @Description 配置内置服务器
 */
@Configuration
@EnableConfigurationProperties(value = { FileProperties.class})
public class ServerConfig {

    @Autowired
    private FileProperties fileProperties;

    @Profile("jetty")
    @Bean
    public EmbeddedServletContainerFactory servletContainer(JettyServerCustomizer jettyServerCustomizer) {
        JettyEmbeddedServletContainerFactory factory =
                new JettyEmbeddedServletContainerFactory();
        factory.addServerCustomizers(jettyServerCustomizer);
        return factory;
    }

    @Bean
    public JettyServerCustomizer jettyServerCustomizer() {
        return server -> {
            // Tweak the connection config used by Jetty to handle incoming HTTP
            // connections
            final QueuedThreadPool threadPool = server.getBean(QueuedThreadPool.class);
            threadPool.setMaxThreads(100);
            threadPool.setMinThreads(20);
        };
    }

    /**
            * 文件上传临时路径
    */
    /*@Bean
    MultipartConfigElement multipartConfigElement() {
        MultipartConfigFactory factory = new MultipartConfigFactory();
        factory.setLocation(fileProperties.getFilePath());
        return factory.createMultipartConfig();
    }*/
}

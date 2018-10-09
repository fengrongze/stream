package com.cst.bigdata.config;

import com.cst.bigdata.config.props.EnvProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Johnney.Chiu
 * create on 2018/2/5 15:14
 * @Description
 * @title
 */
/*@Configuration*/
public class EnvConfiguration {

   /* @Bean(name="envPros")*/
    public static   EnvProperties getEnvPros(){
        return new EnvProperties();
    }

}

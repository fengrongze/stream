package com.cst.jstorm.commons.stream.custom;

import com.cst.jstorm.commons.utils.spring.DefaultCommonSpringConfiguration;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.ImportResource;

/**
 * @author Johnney.chiu
 * create on 2017/12/8 10:56
 * @Description 定制的springcontext config
 */
@Configuration
@ImportResource(value = {"springs/dubbo-hrm-consumer_test.xml"})
@Import(value = {DefaultCommonSpringConfiguration.class})
public class CustomContextTestConfiguration {

}

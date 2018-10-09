package com.cst.jstorm.commons.stream.custom;

import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * @author Johnney.chiu
 * create on 2017/12/29 18:47
 * @Description 低版本的springcontext
 */
public class SpecialApplicationContextCreate {


    public static AbstractApplicationContext getContext(){
       return new ClassPathXmlApplicationContext("springs/spring-httpclient.xml","springs/spring-httpclient.xml");
    }

}

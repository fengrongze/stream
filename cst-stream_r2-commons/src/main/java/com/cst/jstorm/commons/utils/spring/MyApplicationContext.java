package com.cst.jstorm.commons.utils.spring;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.AbstractApplicationContext;

import java.util.Arrays;

/**
 * @author Johnney.chiu
 * create on 2017/11/30 11:40
 * @Description 获取自己的spring context
 */
public class MyApplicationContext {

    public static AbstractApplicationContext getDefaultContext(){
         return new AnnotationConfigApplicationContext(DefaultCommonSpringConfiguration.class);
    }



    public static AbstractApplicationContext getDefineContextWithHttpUtil(Class<?>... clazz){
        Class<?>[] cl=Arrays.copyOf(clazz, clazz.length + 1);
        cl[clazz.length] = DefaultCommonSpringConfiguration.class;
        return new AnnotationConfigApplicationContext(cl);
    }
    public static AbstractApplicationContext getDefineContext(Class<?>... clazz){
        return new AnnotationConfigApplicationContext(clazz);
    }


}

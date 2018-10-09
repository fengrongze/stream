package com.cst.jstorm.commons.stream.custom;

import com.cst.jstorm.commons.utils.spring.MyApplicationContext;
import org.apache.commons.lang.StringUtils;
import org.springframework.context.support.AbstractApplicationContext;

/**
 * @author Johnney.Chiu
 * create on 2018/8/31 11:13
 * @Description
 * @title
 */
public class ComsumerContextSelect {
    public static AbstractApplicationContext getDefineContextWithHttpUtilWithParam(String param){
        if(StringUtils.isBlank(param)||param.equals("prod"))
            return MyApplicationContext.getDefineContextWithHttpUtil(CustomContextConfiguration.class);
        else
            return MyApplicationContext.getDefineContextWithHttpUtil(CustomContextTestConfiguration.class);
    }
}

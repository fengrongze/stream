package com.cst.bigdata.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import springfox.documentation.annotations.ApiIgnore;

/**
 * @author Johnney.chiu
 * create on 2017/11/21 18:19
 * @Description 第一个springboot的类
 */
@RestController
@ApiIgnore
public class HelloSpringBoot {
    @RequestMapping(value = "/springboot",method = RequestMethod.GET)
    public String index() {
        return "Hello SpringBoot";
    }
}

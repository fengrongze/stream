package com.cst.bigdata.config.props;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author Johnney.chiu
 * create on 2017/11/29 16:59
 * @Description 读取args
 */
@Component
public class ArgsProperties {
    @Autowired
    public ArgsProperties(ApplicationArguments args) {
        args.getNonOptionArgs().stream().forEach(System.out::println);
    }
}

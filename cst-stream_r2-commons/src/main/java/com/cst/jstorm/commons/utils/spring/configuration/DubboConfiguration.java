package com.cst.jstorm.commons.utils.spring.configuration;

import com.alibaba.dubbo.config.ApplicationConfig;
import com.alibaba.dubbo.config.ProtocolConfig;
import com.alibaba.dubbo.config.RegistryConfig;
import com.alibaba.dubbo.config.annotation.Reference;
import com.alibaba.dubbo.config.spring.AnnotationBean;
import com.cst.cmds.car.query.service.CarModelQueryService;
import com.cst.cmds.car.query.service.CarQueryService;
import com.cst.jstorm.commons.utils.dubbo.DubboReset;
import org.springframework.context.annotation.Bean;

/**
 * @author Johnney.Chiu
 * create on 2018/5/22 11:42
 * @Description dubboConfig
 * @title
 */
//@Configuration
public class DubboConfiguration {

    @Bean(name="dubboReset")
    public DubboReset createDubboReset(){
        return new DubboReset();
    }

    @Reference
    private CarQueryService carService;

    @Reference
    private CarModelQueryService modelService;

    @Bean
    public ApplicationConfig applicationConfig(DubboReset dubboReset) {
        ApplicationConfig applicationConfig = new ApplicationConfig();
        applicationConfig.setName(dubboReset.getApplicationName());
        applicationConfig.setOwner(dubboReset.getApplicationOwner());
        applicationConfig.setOrganization(dubboReset.getApplicationOrganization());
        return applicationConfig;
    }

    @Bean
    public RegistryConfig registryConfig(DubboReset dubboReset) {
        RegistryConfig registryConfig = new RegistryConfig();
        registryConfig.setAddress(dubboReset.getRegistryAddress());
        registryConfig.setGroup(dubboReset.getRegistryGroup());
        return registryConfig;
    }
    @Bean
    public AnnotationBean annotationBean(DubboReset dubboReset) {
        AnnotationBean annotationBean = new AnnotationBean();
        annotationBean.setPackage(dubboReset.getAnnotationPackage());
        return annotationBean;
    }
    @Bean
    public ProtocolConfig protocolConfig(DubboReset dubboReset) {
        ProtocolConfig protocolConfig = new ProtocolConfig();
        protocolConfig.setPort(dubboReset.getProtocolPort());
        protocolConfig.setName(dubboReset.getProtocolName());
        return protocolConfig;
    }



}

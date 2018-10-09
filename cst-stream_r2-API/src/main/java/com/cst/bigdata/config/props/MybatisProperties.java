package com.cst.bigdata.config.props;

import lombok.*;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author Johnney.chiu
 * create on 2017/12/11 16:30
 * @Description test
 */
@ConfigurationProperties(prefix = "mybatis")
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
public class MybatisProperties {

    private String mapperLocations;

    private String typeAliasesPackage;

    private String myName;

}

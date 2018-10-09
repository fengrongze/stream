package com.cst.bigdata.config.props;

import lombok.*;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author Johnney.Chiu
 * create on 2018/6/22 15:01
 * @Description big dao properties
 * @title
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
@ConfigurationProperties(prefix = "bigdao")
public class BigDaoProperties {

    private String gdcpFactoryClientSystem;

    private String gdcpFactoryClientProxyUrl;
}

package com.cst.bigdata.config.props;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author Johnney.chiu
 * create on 2017/11/23 14:15
 * @Description 连接hbase的属性
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ConfigurationProperties(prefix = "hbase")
public class HbaseProperties {

    private String zkQuorum;

    private String zkPort;

    private String rootDir;

    private String zkDataDir;

    private String znodeParent;


}

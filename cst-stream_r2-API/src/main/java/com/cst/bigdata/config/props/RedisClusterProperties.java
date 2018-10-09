package com.cst.bigdata.config.props;

import lombok.*;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author Johnney.Chiu
 * create on 2018/5/9 15:58
 * @Description redisProperties
 * @title
 */

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
@ConfigurationProperties(prefix = "myrediscluster")
public class RedisClusterProperties {

    private String hosts;
    private int maxTotal;
    private int maxIdle;
    private int minIdle;
    private int timeout;
    private int maxRedirections;


}

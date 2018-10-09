package com.cst.bigdata.config;

import com.cst.bigdata.config.props.RedisClusterProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Johnney.Chiu
 * create on 2018/5/9 16:08
 * @Description RedisClusterconfig
 * @title
 */
@Configuration
@EnableConfigurationProperties(value = {RedisClusterProperties.class})
@Slf4j
public class RedisClusterConfig {

    @Autowired
    private RedisClusterProperties redisClusterProperties;

    @Bean("hJedisCluster")
    public JedisCluster createJedisCluster(){
        if (StringUtils.isBlank(redisClusterProperties.getHosts())) {
            log.info("this hosts is null");
            return null;
        }
        Set<HostAndPort> hosts=new HashSet<>();
        for(String str : redisClusterProperties.getHosts().split(",")){
            if(StringUtils.isNotBlank(str)){
                String[] hp=str.split(":");
                hosts.add(new HostAndPort(hp[0], Integer.valueOf(hp[1])));
            }
        }
        GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
        genericObjectPoolConfig.setMaxTotal(redisClusterProperties.getMaxTotal());
        genericObjectPoolConfig.setMinIdle(redisClusterProperties.getMinIdle());
        genericObjectPoolConfig.setMaxIdle(redisClusterProperties.getMinIdle());

        return new JedisCluster(hosts, redisClusterProperties.getTimeout(),
                    redisClusterProperties.getTimeout(), genericObjectPoolConfig);
    }


}

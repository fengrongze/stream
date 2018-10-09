package com.cst.jstorm.commons.utils;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * @author Johnney.chiu
 * create on 2017/11/24 18:34
 * @Description
 */
public class RedisUtil {
    private static final Logger logger = LoggerFactory.getLogger(RedisUtil.class);
    private static final String LOCK_SUCCESS = "OK";
    private static final String SET_IF_NOT_EXIST = "NX";
    private static final String SET_WITH_EXPIRE_TIME = "PX";
    private static final Long RELEASE_SUCCESS = 1L;
    public static final int EXPIRETIME = 2;

    /**
     * 尝试获取分布式锁
     * @param jedis Redis客户端
     * @param lockKey 锁
     * @param requestId 请求标识
     * @param expireTime 超期时间
     * @return 是否获取成功
     */
    public static boolean tryGetDistributedLock(JedisCluster jedis, String lockKey, String requestId, int expireTime) {

        String result = jedis.set(lockKey, requestId, SET_IF_NOT_EXIST, SET_WITH_EXPIRE_TIME, expireTime);

        if (LOCK_SUCCESS.equals(result)) {
            return true;
        }
        return false;

    }


    /**
     * 释放分布式锁
     * @param jedis Redis客户端
     * @param lockKey 锁
     * @param requestId 请求标识
     * @return 是否释放成功
     */
    public static boolean releaseDistributedLock(JedisCluster jedis, String lockKey, String requestId) {

        String script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
        Object result = jedis.eval(script, Collections.singletonList(lockKey), Collections.singletonList(requestId));

        if (RELEASE_SUCCESS.equals(result)) {
            return true;
        }
        return false;

    }

    public static JedisCluster buildJedisCluster(Properties prop, String rKey) {
        if(rKey==null||"".equals(rKey))
            return null;

        String redisHosts=  prop.getProperty(rKey);
        Set<HostAndPort> hosts=new HashSet<>();
        for(String str : redisHosts.split(",")){
            if(str!=null){
                String[] hp=str.split(":");
                hosts.add(new HostAndPort(hp[0],Integer.valueOf(hp[1])));
            }
        }
        logger.debug("creeate redis+"+hosts.toString());
        GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
        genericObjectPoolConfig.setMaxTotal(Integer.parseInt(prop.getProperty("storm.rediscluster.maxTotal","200")));
        genericObjectPoolConfig.setMaxIdle(Integer.parseInt(prop.getProperty("storm.rediscluster.maxIdle","40")));
        genericObjectPoolConfig.setMinIdle(Integer.parseInt(prop.getProperty("storm.rediscluster.minIdle","10")));
        int timeout = Integer.parseInt(prop.getProperty("storm.rediscluster.timeout","4000")),
                redirections = Integer.parseInt(prop.getProperty("storm.rediscluster.maxRedirections","5"));
        return new JedisCluster(hosts, timeout, redirections, genericObjectPoolConfig);
    }



}

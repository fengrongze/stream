package com.cst.jstorm.commons.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCluster;

/**
 * @author Johnney.chiu
 * create on 2017/11/28 17:38
 * @Description 业务锁
 */
public class CacheTemplateBusinessLocker {
    private static final Logger logger = LoggerFactory.getLogger("CacheTemplateBusinessLocker");

    /**
     * 重试次数
     */
    private int retryTimes = 1;

    /**
     * 重试间隔时间，单位：ms
     */
    private long retryDelay = 100;
    /**
     * 锁最长持续时间(s)
     */
    private int expireTime = 60;

    /**
     * 获取锁超时时间 ms
     */
    private long acquireTimeoutInMS=60000;

    private JedisCluster jedis;

    public CacheTemplateBusinessLocker(JedisCluster jedis) {
        this.jedis = jedis;
    }
    public boolean getBusinessLock(String bussinessCode, String key) {
        if(null==jedis.get(key))
            return lock(bussinessCode + key);
        return false;

    }
    public boolean getBusinessLockRetry(String bussinessCode, String key) {
        int count = 0;
        while(count<retryTimes){
            if(getBusinessLock(bussinessCode, key)){
                return true;
            }
            count++;
            logger.warn("bussinessCode:"+bussinessCode+"key:"+key+"，num:"+count+"failed。");
            try {
                logger.warn("bussinessCode:"+bussinessCode+",can't get "+key+"，begin sleep:"+retryDelay);
                Thread.sleep(retryDelay);
            } catch (InterruptedException e) {
                logger.warn("bussinessCode:"+bussinessCode+",sleep Interrup go on check");
            }
        }
        logger.debug("bussinessCode:"+bussinessCode+" retry "+retryTimes+" times can't get key:"+key);
        return false;
    }


    public void unlock(String bussinessCode, String key) {
        JedisCluster jedis = null;
        try {
            jedis.del(bussinessCode + key);
        } catch (Exception e) {
            logger.warn("locker " + key + "release fail!", e);
        }
    }

    private boolean lock(final String key){
        JedisCluster jedis = null;
        try {
            long now = System.currentTimeMillis();
            String expireStr = String.valueOf(now + acquireTimeoutInMS);
            //如果设置成功，设置业务锁

            if (jedis.setnx(key, expireStr) == 1) {
                jedis.expire(key, expireTime);
                return true;
            }
            //如果key存在,但没有设置的expire time，或者失效,重新删除并设置业务锁
            if (jedis.ttl(key) == -1) {
                jedis.del(key);
                return lock(key);
            }
            String val = jedis.get(key);
            //取值判断，如果当前值>缓存的key
            if (now > Long.parseLong(val)) {
                jedis.del(key);
                return lock(key);
            }

            return false;
        }catch (Exception e){
            logger.warn("locker is aready locked:" + key,e);
            return false;
        }

    }




    public int getRetryTimes() {
        return retryTimes;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }

    public long getRetryDelay() {
        return retryDelay;
    }

    public void setRetryDelay(long retryDelay) {
        this.retryDelay = retryDelay;
    }

    public int getExpireTime() {
        return expireTime;
    }

    public void setExpireTime(int expireTime) {
        this.expireTime = expireTime;
    }

    public long getAcquireTimeoutInMS() {
        return acquireTimeoutInMS;
    }

    public void setAcquireTimeoutInMS(long acquireTimeoutInMS) {
        this.acquireTimeoutInMS = acquireTimeoutInMS;
    }
}

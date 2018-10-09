package com.cst.bigdata.service.integrate;

import com.cst.stream.common.DateTimeUtil;
import com.cst.stream.common.JsonHelper;
import com.cst.stream.common.StreamRedisConstants;
import com.cst.stream.stathour.CSTData;
import com.fasterxml.jackson.core.type.TypeReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;

import static com.cst.stream.common.StreamRedisConstants.GasKey.NIGHTY_THREE_GAS;

/**
 * @author Johnney.Chiu
 * create on 2018/6/28 18:46
 * @Description
 * @title
 */
@Service
@Slf4j
public class RedisCommonService<L extends CSTData,S extends CSTData> {
    /**
     * 通过缓存获取最近小时数据
     * @param head 缓存类型
     * @param carId carid
     * @return
     * @throws IOException
     */

    @Autowired
    private JedisCluster jedisCluster;

    public L getLatestDataFromRedis(String head,String carId){
        String latestKey = StreamRedisConstants.StreamRedisFormat.getLatestZoneRedisKey(head, carId);
        String latestData = jedisCluster.get(latestKey);
        if(latestData==null) {
            return null;
        }
        return JsonHelper.toBeanWithoutException(latestData, new TypeReference<L>() {
        });

    }
    public S getFirstDataFromRedis(String head,String carId,String timeZone){
        String firstKey = StreamRedisConstants.StreamRedisFormat.getFirstZoneRedisKey(head,carId,timeZone);
        String firstData = jedisCluster.get(firstKey);
        if(firstData==null) {
            return null;
        }
        return JsonHelper.toBeanWithoutException(firstData, new TypeReference<S>() {
        });

    }


    public String getGasNumFromRedis(String carId){
        return jedisCluster.get(StreamRedisConstants.GasKey.getGasNumKey(carId));
    }

    private  String getCarCityFromRedis(String carId){
        return jedisCluster.get(StreamRedisConstants.GasKey.getCarCityKey(carId));
    }


    private String getGasPriceFromRedis(String gasNum,String city,long timestamp){
        String dateTimeStr= DateTimeUtil.toLongTimeString(timestamp, DateTimeUtil.DEFAULT_SHORT_DATE_DEFULT);
        return jedisCluster.get(StreamRedisConstants.GasKey.getGasPriceKey(dateTimeStr, city, gasNum));
    }

    public float calcPrice(String carId,long timestamp){
        String gasNum = getGasNumFromRedis(carId);
        if (StringUtils.isBlank(gasNum)) {
            gasNum= NIGHTY_THREE_GAS;
        }
        String carCity = getCarCityFromRedis(carId);
        if (StringUtils.isBlank(carCity)) {
            carCity = "重庆";
        }

        String gasPrice= getGasPriceFromRedis(gasNum, carCity, timestamp);

        return NumberUtils.toFloat(gasPrice, 7.8f);


    }

}

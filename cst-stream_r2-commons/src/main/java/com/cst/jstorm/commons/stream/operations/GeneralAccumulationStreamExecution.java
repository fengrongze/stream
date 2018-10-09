package com.cst.jstorm.commons.stream.operations;

import com.cst.jstorm.commons.stream.constants.OtherKey;
import com.cst.jstorm.commons.stream.operations.hbasestrategy.IHBaseQueryAndPersistStrategy;
import com.cst.jstorm.commons.stream.operations.hbasestrategy.TimeSelectRowKeyGrenerate;
import com.cst.jstorm.commons.utils.exceptions.NoSourceDataException;
import com.cst.stream.common.*;
import com.cst.stream.stathour.CSTData;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.cst.jstorm.commons.stream.constants.RedisKey.ExpireTime.MONTH_ZONE_VALUE_EXPIRE_TIME;
import static com.cst.stream.common.DateTimeUtils.parseTime;

/**
 * @author yangxu
 * create on 2018/06/11 10:34
 * @Description 天数据处理流程
 */
public class GeneralAccumulationStreamExecution<S extends CSTData, T extends CSTData,
        C extends DataAccumulationTransforInterface<T, S>> {
    protected String lastDataKey;
    protected S s;
    protected T t;
    protected C c;
    protected JedisCluster jedis;

    Logger logger = LoggerFactory.getLogger(GeneralAccumulationStreamExecution.class);

    /**
     * 天数据处理
     * @param map
     * @param resultDataQuery
     * @throws Exception
     */
    public void dealAccumulationData(Map map, IHBaseQueryAndPersistStrategy<T> resultDataQuery) throws Exception {


        T latestTransforData = findLatestData(map,resultDataQuery,s.getCarId(),s.getTime());
        //检验数据的正确与否
        T transforData;

        try {
            if (latestTransforData == null) {
                //缓存中没有查找到最近一条数据
                logger.debug("data is null,init data!");
                //将当前数据作为最近一条数据补充到最近数据
                transforData = c.initTransforDataBySource(s,map);
            }else{
                transforData = c.calcTransforData(latestTransforData, s, map);
            }
            logger.info("my latest data is {},tranforData is {},source data is {}",latestTransforData,transforData,s);

            persistCurrentData(transforData,map,resultDataQuery,latestTransforData);
        }catch (Throwable e){
            logger.error("calc day data source {},latest data {}",s,latestTransforData,e);
        }
    }

    /**
     * 持久化当今计算结果
     * @param transforData
     * @param map
     * @param resultDataQuery
     * @param latestTransforData
     */
    private void persistCurrentData(T transforData,Map map,IHBaseQueryAndPersistStrategy<T> resultDataQuery,T latestTransforData)throws Exception{
        CstConstants.TIME_SELECT timeSelect= (CstConstants.TIME_SELECT)map.get(OtherKey.DataDealKey.TIME_SELECT);
        DateTimeUtils.TimeFormat timeFormat=(DateTimeUtils.TimeFormat) map.get(OtherKey.MIDLLE_DEAL.TIME_FORMAT);
        List<String> persistList=(LinkedList)map.get(OtherKey.MIDLLE_DEAL.PERSIST_KEY);
        if(persistList==null) {
            persistList = new LinkedList<>();
            map.put(OtherKey.MIDLLE_DEAL.PERSIST_KEY, persistList);
        }
        prisistData(transforData, persistList);
        persistLatestDataToRedis(lastDataKey,transforData, NumberUtils.toInt((String)map.get(OtherKey.MIDLLE_DEAL.ZONE_VALUE_EXPIRE_TIME),
                MONTH_ZONE_VALUE_EXPIRE_TIME));
        Long latestTime  = latestTransforData==null?null:latestTransforData.getTime();
        if(null==latestTime){
            latestTime = transforData.getTime();
        }
        List<String> timeArea=DateTimeUtils.getBetweenDateStr(latestTime, s.getTime(), timeSelect);
        if(null!=timeArea && timeArea.size()>0){
            for(String time:timeArea){
                map.put(OtherKey.DataDealKey.ROWKEY_GENERATE, new TimeSelectRowKeyGrenerate<T>(s.getCarId(),
                        DateTimeUtils.getMilliByTime(parseTime(time, timeFormat)),
                        timeSelect));
                //从hbse中获取天数据
                T dayHBaseData = resultDataQuery.findHBaseData(map);
                //仅仅需要判断第一个数据
                if(dayHBaseData==null) {
                    dayHBaseData = c.initTransforDataByLatest(latestTransforData, map,
                            DateTimeUtils.getMilliByTime(parseTime(time, timeFormat)));
                    //仅仅补全区间数据，不纳入下次计算
                    prisistData(dayHBaseData ,persistList);
                }
                else {
                    break;
                }
            }
        }


    }

    /**
     * 转换并持久化数据
     * @param transforData
     * @param persistList
     */
    private void prisistData(T transforData, List<String> persistList) {
        String dataTmp = JsonHelper.toStringWithoutException(transforData);
        persistList.add(dataTmp);
    }

    private <K extends CSTData> void persistLatestDataToRedis(String latestRedis,K data,int expireTime)throws Exception{
        String dataTmp =JsonHelper.toStringWithoutException(data);
        jedis.setex(latestRedis,expireTime, dataTmp);
        logger.debug("last data persist to redis is {}",dataTmp);
    }

    /**
     * 查询最新计算结果数据,先查询缓存,没有数据查询hbase
     * @param map
     * @param resultDataQuery
     * @param carId
     * @param time
     * @return
     * @throws Exception
     */
    private T findLatestData(Map map,IHBaseQueryAndPersistStrategy<T> resultDataQuery,String carId,Long time)throws Exception{
        T latestData = findRedisData(lastDataKey);
        if(null==latestData){
            latestData = findHbaseLatestData(map,resultDataQuery,carId,time);
        }
        return latestData;
    }

    /**
     * 从hbse中获取当前天数据统计
     * @param map
     * @param ihBaseQueryAndPersistStrategy
     * @param carId
     * @param time
     * @param <T>
     * @return
     */
    private  <T extends CSTData> T findHbaseLatestData(Map map,IHBaseQueryAndPersistStrategy<T> ihBaseQueryAndPersistStrategy,String carId,Long time){
        map.put(OtherKey.DataDealKey.ROWKEY_GENERATE, new TimeSelectRowKeyGrenerate<T>(carId, time,
                (CstConstants.TIME_SELECT) map.get(OtherKey.DataDealKey.TIME_SELECT)));
        return ihBaseQueryAndPersistStrategy.findHBaseData(map);
    }
    /**
     * 查询最近条数据
     * @param latestRedis
     * @return
     * @throws Exception
     */
    private <K extends CSTData> K findLatestDataFromRedis(String latestRedis)throws Exception{
        K latestZoneData = findRedisData(latestRedis);
        return latestZoneData;
    }
    /**
     * 查询当前时区中是否存在第一条数据
     * @param redisKey
     * @param <K>
     * @return
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    private <K extends CSTData> K findRedisData(String redisKey)  throws Exception{
        String redisDataTemp = jedis.get(redisKey);
        //当前缓存时区中没有数据
        if (StringUtils.isEmpty(redisDataTemp)) {
            return null;
        }
        //缓存中存在数据
        K redisData = JsonHelper.toBeanWithoutException(redisDataTemp, new TypeReference<K>(){});
        logger.debug("redis zone data is {}",redisData);
        return redisData;
    }
    public GeneralAccumulationStreamExecution createJedis(JedisCluster jedis) {
        this.jedis = jedis;
        return this;
    }

    public  <N extends CSTData> GeneralAccumulationStreamExecution createLatestRedis(String head, N needData, String fmt)throws NoSourceDataException {
        lastDataKey = genLatestRedis(head, needData,fmt);
        return this;
    }
    private <N extends CSTData>  String genLatestRedis(String head,N needData, String fmt)throws NoSourceDataException {
        return  StreamRedisConstants.StreamRedisFormat.getLatestZoneTimeRedisKey(head, needData.getCarId(), DateTimeUtil.toLongTimeString(needData.getTime(), fmt));
    }

    public GeneralAccumulationStreamExecution createSpecialCalc(C c) {
        this.c = c;
        return this;
    }

    public GeneralAccumulationStreamExecution createSpecialSource(String str, String zone_key
            , String fmt) throws NoSourceDataException, IOException {
        if (StringUtils.isEmpty(str)) {
            throw new NoSourceDataException("this data is empty");
        }
        s = JsonHelper.toBeanWithoutException(str, new TypeReference<S>(){});
        logger.debug("day data str:{} ,s:{}",str,s);
        try {
            createLatestRedis(zone_key,s,fmt);
        }catch (NoSourceDataException e){
            throw e;
        }
        return this;
    }

    public String gentMsgId(){
        return (s==null)?"default" : s.getCarId();
    }
}

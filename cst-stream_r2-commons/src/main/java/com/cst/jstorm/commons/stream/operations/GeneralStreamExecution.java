package com.cst.jstorm.commons.stream.operations;

import com.cst.jstorm.commons.stream.constants.OtherKey;
import com.cst.jstorm.commons.stream.constants.RedisKey;
import com.cst.jstorm.commons.stream.operations.hbasestrategy.IHBaseQueryAndPersistStrategy;
import com.cst.jstorm.commons.stream.operations.hbasestrategy.NoDelayRowKeyGenerate;
import com.cst.jstorm.commons.stream.operations.hbasestrategy.TimeSelectRowKeyGrenerate;
import com.cst.jstorm.commons.utils.exceptions.NoSourceDataException;
import com.cst.stream.common.CstConstants;
import com.cst.stream.common.DateTimeUtil;
import com.cst.stream.common.JsonHelper;
import com.cst.stream.stathour.CSTData;
import com.cst.stream.stathour.LastTimeData;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;
import java.text.ParseException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.cst.jstorm.commons.stream.constants.OtherKey.MIDLLE_DEAL.LAST_DATA_PARAM;
import static com.cst.stream.common.DateTimeUtil.ONE_HOUR;
import static com.cst.stream.common.StreamRedisConstants.ExpireTime.LAST_CALC_VALUE_EXPIRE_TIME;
import static com.cst.stream.common.StreamRedisConstants.ExpireTime.ZONE_VALUE_EXPIRE_TIME;

/**
 * @author Johnney.chiu
 * create on 2017/12/1 10:34
 * @Description 小时处理流程
 */
public class GeneralStreamExecution<S extends CSTData, T extends CSTData,
        C extends DataTransforInterface<T, S>> {
    protected String jedisUnicleKey;
    protected String lastTimeKey;
    protected S s;
    protected T t;
    protected C c;
    protected JedisCluster jedis;





    Logger logger = LoggerFactory.getLogger(GeneralStreamExecution.class);


    /**
     * 从时区缓存key-jedisUnicleKey缓存中获取当前时区的缓存值，
     * 如果没有（没有的情况出现在：该时间区没有任何的记录或者缓存过期），直接返回，并执行clacHbaseValue
     * 如果存在，直接用来计算
     * 计算完成后,抛出数据
     * @param map
     * @return map
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    public boolean findZoneValueFromRedis(Map map)  throws Exception{
        map.put("uploadTime", s.getTime());
        String redisDataTemp = jedis.get(jedisUnicleKey);
        //当前缓存时区中没有数据
        if (StringUtils.isEmpty(redisDataTemp)) {
            return false;
        }
        //缓存中存在数据
        t = JsonHelper.toBeanWithoutException(redisDataTemp, new TypeReference<T>() {
        });
        //可能存在空数据，或者deserialize json失败
        if (t == null)
            return false;
        logger.debug("redis zone data is {}",t);
        return true;

    }

    /**
     * 当从时区缓存中没有获取到数据时，处理这个方法
     * 如果从hbase(cst_stream_hour_statistics或者cst_stream_day_statistics)中没有找到数据，直接返回，并执行clacLastKeyValue
     * 如果找到数据，说明数据仍然在计算的时区之内，计算并抛出数据
     *
     * @param map
     * @return map
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    public boolean findZoneValueFromHbase( Map map,IHBaseQueryAndPersistStrategy<T> ihBaseQueryAndPersistStrategy) throws Exception {
        map.put("uploadTime", s.getTime());
        findHbaseZone(map, ihBaseQueryAndPersistStrategy);
        if(t==null) {
            return false;
        }
        logger.debug("hbase zone data is {}",t);
        return true;

    }

    public void calcZoneData(Map map,IHBaseQueryAndPersistStrategy<T> ihBaseQueryAndPersistStrategy)throws Exception{
        /*if(s.toString().contains("6a8cc3ab01bf485a95c3181d1d5be02a"))
            logger.info("source data is {}",s);*/
        //查找缓存中的区间数据，说明缓存要么失效或者缓存中没有时区数据
        if(!findZoneValueFromRedis(map))
            //如果可能缓存失效了，再次从hbase中查找数据，如果hbase中依然不存在数据，那就说明确实没有时区数据
            findZoneValueFromHbase(map, ihBaseQueryAndPersistStrategy);


    }

    private void zoneDataRedis(Map map,T data)  {
        String dataTmp = JsonHelper.toStringWithoutException(data);
        int expireTime = NumberUtils.toInt((String)map.get(OtherKey.MIDLLE_DEAL.ZONE_VALUE_EXPIRE_TIME),
                ZONE_VALUE_EXPIRE_TIME);
        jedis.setex(jedisUnicleKey,expireTime, dataTmp);
        logger.debug("persist zone data to redis is {}",dataTmp);
    }

    private void zoneDataPersist(Map map,T data) throws  ParseException {
        List<String> persistList=(LinkedList)map.get(OtherKey.MIDLLE_DEAL.PERSIST_KEY);
        if(persistList==null) {
            persistList = new LinkedList<>();
            map.put(OtherKey.MIDLLE_DEAL.PERSIST_KEY, persistList);
        }
        String dataTmp = JsonHelper.toStringWithoutException(data);
        persistList.add(dataTmp);
        logger.debug("persist zone data to hbase is {}",dataTmp);
    }

    private void dataToNextStream(Map map,T lastData)throws ParseException{
        if(lastData==null||s.getTime()<lastData.getTime())
            return;
        String nextStream = (String) map.get(OtherKey.MIDLLE_DEAL.NEXT_STREAM);
        if(StringUtils.isEmpty(nextStream))
            return;
        //每小时的计算结果纳入下次作为计算
        if (!DateTimeUtil.dateTimeDifferentBetween(lastData.getTime(), s.getTime(),  DateTimeUtil.DEFAULT_DATE_HOUR)) {
            logger.info("now data is {},datetime:{},source data is {},datetime:{}",lastData,DateTimeUtil.toLongTimeString(lastData.getTime(),DateTimeUtil.DEFAULT_DATE_DEFULT),s,DateTimeUtil.toLongTimeString(s.getTime(),DateTimeUtil.DEFAULT_DATE_DEFULT));
            List<String> nextList=(List<String>)map.get(OtherKey.MIDLLE_DEAL.NEXT_KEY);
            String dataTmp = JsonHelper.toStringWithoutException(lastData);
                nextList.add(dataTmp);
        }
    }

    /**
     *
     * @param map
     * @return
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    //String nextStream, String fmt, long interval
    public void clacLastKeyValue( Map map,
            IHBaseQueryAndPersistStrategy<T> iHBaseQueryAndPersistStrategy,
                                  IHBaseQueryAndPersistStrategy<T> iHbaseFindZoneStrategy) throws Exception {

        map.put("uploadTime", s.getTime());
        map.put(OtherKey.DataDealKey.ROWKEY_GENERATE, new NoDelayRowKeyGenerate<>(s.getCarId(),(String)map.get(LAST_DATA_PARAM)));
        //缓存查找数据
        String lastValue = jedis.get(lastTimeKey);
        T lastData;
        //如果数据为空
        if (StringUtils.isEmpty(lastValue)) {
            //查询实时数据hbase表
            lastData = iHBaseQueryAndPersistStrategy.findHBaseData(map);
        }else{
            LastTimeData<T> lastTimeData = JsonHelper.toBeanWithoutException(lastValue, new TypeReference<LastTimeData<T>>() {
                    }
            );
            lastData = lastTimeData.getData();
        }
        // 重复Rebalance的数据
        /*if (lastData != null && s.getTime() == lastData.getTime()) {
            logger.warn("Disorder time data, new: {}, old: {}", s, lastData);
            return;
        }*/
        //如果没有时区数据，那么初始化当前的时区的数据，并且判断数据是否跨区
        if(t==null) {
        //处理是否跨区的问题
            if (lastData != null) {
                List<String> list = DateTimeUtil.calcLongTimeBetween(lastData.getTime(), s.getTime(), DateTimeUtil.DEFAULT_DATE_HOUR, ONE_HOUR);
                //紧挨着的时区才用最近的数据做初始化
                if(list.size()==0&&lastData.getTime()<s.getTime())
                    clacInitValue(map, lastData);
                else
                    clacInitValue(map);
            } else {
                clacInitValue(map);
            }
        } else {
            c.execute(t, s, map);
        }
        //处理当前数据是否要到天
        dataToNextStream(map, lastData);
        //数据处理完后，存缓存，持久化
        zoneDataRedis(map,t);
        zoneDataPersist(map, t);
        if(lastData!=null){
            calcZoneData(map, iHbaseFindZoneStrategy, lastData);
        }
        //如果传入数据小于当前时间就不在做实时数据存入
        if(lastData==null||s.getTime()>lastData.getTime())
            lastDataRedis(map, t);

    }
    private void lastDataRedis(Map map,T data) {
        LastTimeData<T> lastTimeData = new LastTimeData<>(data.getTime(), data);
        String dataTmp = JsonHelper.toStringWithoutException(lastTimeData);
        int expireTime = NumberUtils.toInt((String)map.get(OtherKey.MIDLLE_DEAL.ZONE_VALUE_EXPIRE_TIME),
                LAST_CALC_VALUE_EXPIRE_TIME);
        jedis.setex(lastTimeKey,expireTime, dataTmp);
        //最近数据持久化到hbase
        map.put(OtherKey.MIDLLE_DEAL.NO_DELAY_TIME_ZONE, dataTmp);
        logger.debug("last data persist to redis is {}",dataTmp);
    }
    //初始化
    @SuppressWarnings("unchecked")
    private void clacInitValue(Map map) {
        map.put("uploadTime", s.getTime());
        t = (T) c.init(s, map);
    }
    private void clacInitValue(Map map,T lastData)   {
        map.put("uploadTime", s.getTime());
        t = (T) c.initOffet(lastData, s, map);
    }

    private void calcZoneData(Map map,IHBaseQueryAndPersistStrategy<T> iHbaseFindZoneStrategy,T lastData) throws  ParseException {
        //比对区间，如果在同一区间,（在同一区间，只需要save到缓存，同时更新最近计算数据）

        long interval = (long) map.get(OtherKey.MIDLLE_DEAL.INTERVAL);
        String fmt = (String) map.get(OtherKey.MIDLLE_DEAL.FMT);
        //数据在小时变化的时候就下发数据到下一个bolt处理

        if (!DateTimeUtil.dateTimeDifferentBetween(t.getTime(), lastData.getTime(), fmt)) {
            List<String> timeArea = DateTimeUtil.calcLongTimeBetween(t.getTime(), lastData.getTime(), fmt, interval);
            if (CollectionUtils.isNotEmpty(timeArea) && timeArea.size() > 30) {
                logger.warn("To much cross zone data: size={}, old: {}, real-time: {}, new: {}", timeArea.size(), t, lastData, s);
            }
            //补齐之间的时间区域到持久化中
            if (!timeArea.isEmpty()) {
                List<String> persistList=(LinkedList)map.get(OtherKey.MIDLLE_DEAL.PERSIST_KEY);
                if(persistList==null) {
                    persistList = new LinkedList<>();
                    map.put(OtherKey.MIDLLE_DEAL.PERSIST_KEY, persistList);
                }
                for (String str : timeArea) {
                    map.put("uploadTime", DateTimeUtil.strToTimestamp(str, fmt));
                    //以当前时区去获取数据，如果有则不做处理，如果没有则初始化
                    map.put(OtherKey.DataDealKey.ROWKEY_GENERATE, new TimeSelectRowKeyGrenerate<T>(s.getCarId(), DateTimeUtil.strToTimestamp(str, fmt),
                            (CstConstants.TIME_SELECT) map.get(OtherKey.DataDealKey.TIME_SELECT)));
                    //从hbse中获取区间数据
                    T zoneStrategyHBaseData = iHbaseFindZoneStrategy.findHBaseData(map);
                    //仅仅需要判断第一个数据
                    if(zoneStrategyHBaseData==null) {
                        zoneStrategyHBaseData = (T) c.initFromTransfer(lastData, map);
                        //仅仅补全区间数据，不纳入下次计算
                        persistList.add(JsonHelper.toStringWithoutException(zoneStrategyHBaseData));
                    }
                    else
                        break;

                }
            }
        }


    }


    public  void findHbaseZone(Map map,IHBaseQueryAndPersistStrategy<T> ihBaseQueryAndPersistStrategy){
        map.put(OtherKey.DataDealKey.ROWKEY_GENERATE, new TimeSelectRowKeyGrenerate<T>(s.getCarId(), s.getTime(),
                (CstConstants.TIME_SELECT) map.get(OtherKey.DataDealKey.TIME_SELECT)));
        //从hbse中获取区间数据
        t = ihBaseQueryAndPersistStrategy.findHBaseData(map);
    }
    public GeneralStreamExecution createSpecialSource(S s) throws NoSourceDataException, IOException {
        if (s == null)
            throw new NoSourceDataException("this data is empty");
        this.setS(s);
        return this;
    }

    public GeneralStreamExecution createSpecialCalc(C c) {
        this.c = c;
        return this;
    }
    public GeneralStreamExecution createSpecialSource(String str) throws NoSourceDataException, IOException {
        if (StringUtils.isEmpty(str))
            throw new NoSourceDataException("this data is empty");
        s = JsonHelper.toBeanWithoutException(str, new TypeReference<S>() {
        });
        return this;
    }
    public GeneralStreamExecution createSpecialSource(String str,String zone_key
            ,String fmt,String lastTimeKey) throws NoSourceDataException, IOException {
        if (StringUtils.isEmpty(str))
            throw new NoSourceDataException("this data is empty");
        s =JsonHelper.toBeanWithoutException(str, new TypeReference<S>() {
        });
        try {
            createJedisUnicleKey(zone_key, fmt);
            createLastTimeKey(lastTimeKey);

        }catch (NoSourceDataException e){
            throw e;
        }
        return this;
    }

    public String getSpecialSourceStr(S s)  {
        return  JsonHelper.toStringWithoutException(s);
    }


    public GeneralStreamExecution createJedisUnicleKey(String head, String fmt) throws NoSourceDataException {
        if (s == null)
            throw new NoSourceDataException("calc data is null,createJedisUnicleKey error");
        String mod = s.getCarId();
        jedisUnicleKey = head + RedisKey.KEY_SEP+ mod +RedisKey.KEY_SEP+ DateTimeUtil.toLongTimeString(s.getTime(), fmt);
        return this;
    }

    public GeneralStreamExecution createLastTimeKey(String head)throws NoSourceDataException {
        if (s == null)
            throw new NoSourceDataException("calc data is null,createJedisUnicleKey error");
        String mod = s.getCarId();
        lastTimeKey = mod + RedisKey.KEY_SEP+ head;
        return this;
    }

    public GeneralStreamExecution createJedis(JedisCluster jedis) {
        this.jedis = jedis;
        return this;
    }
    @SuppressWarnings("unchecked")
    public LastTimeData getNewLastTimeData() {
        return new LastTimeData(s.getTime(), t);
    }




    public String gentMsgId(){
        return (s==null)?"default" : s.getCarId();
    }

    public String getJedisUnicleKey() {
        return jedisUnicleKey;
    }

    public void setJedisUnicleKey(String jedisUnicleKey) {
        this.jedisUnicleKey = jedisUnicleKey;
    }

    public String getLastTimeKey() {
        return lastTimeKey;
    }

    public void setLastTimeKey(String lastTimeKey) {
        this.lastTimeKey = lastTimeKey;
    }

    public S getS() {
        return s;
    }

    public void setS(S s) {
        this.s = s;
    }

    public T getT() {
        return t;
    }

    public void setT(T t) {
        this.t = t;
    }

    public JedisCluster getJedis() {
        return jedis;
    }

    public void setJedis(JedisCluster jedis) {
        this.jedis = jedis;
    }


}

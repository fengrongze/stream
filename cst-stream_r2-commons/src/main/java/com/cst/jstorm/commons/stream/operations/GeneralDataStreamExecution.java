package com.cst.jstorm.commons.stream.operations;

import com.cst.jstorm.commons.stream.constants.ExceptionCodeStatus;
import com.cst.jstorm.commons.stream.constants.OtherKey;
import com.cst.jstorm.commons.stream.operations.hbasestrategy.IHBaseQueryAndPersistStrategy;
import com.cst.jstorm.commons.stream.operations.hbasestrategy.TimeSelectAndTypeRowKeyGrenerate;
import com.cst.jstorm.commons.stream.operations.hbasestrategy.TimeSelectRowKeyGrenerate;
import com.cst.jstorm.commons.utils.exceptions.NoSourceDataException;
import com.cst.stream.common.CstConstants;
import com.cst.stream.common.DateTimeUtil;
import com.cst.stream.common.JsonHelper;
import com.cst.stream.common.StreamRedisConstants;
import com.cst.stream.stathour.CSTData;
import com.fasterxml.jackson.core.type.TypeReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;
import java.text.ParseException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.cst.jstorm.commons.stream.constants.RedisKey.ExpireTime.ZONE_SCHEDULE_DEALT_EXPIRE_TIME;
import static com.cst.jstorm.commons.stream.constants.RedisKey.ExpireTime.ZONE_VALUE_EXPIRE_TIME;

/**
 * @author Johnney.Chiu
 * create on 2018/5/31 11:11
 * @Description xiaoqtest
 * @title
 */
@Slf4j
public class GeneralDataStreamExecution<S extends CSTData, T extends CSTData,L extends CSTData,
        C extends DataDealTransforInterface<T, S, L>> {

    protected String firstZoneRedis;
    protected String resultZoneRedis;
    protected String latestRedis;
    protected S s;
    protected C c;
    protected T t;
    protected JedisCluster jedis;


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
        K redisData = JsonHelper.toBeanWithoutException(redisDataTemp, new TypeReference<K>() {
        });
        log.debug("redis zone data is {}",redisData);
        return redisData;
    }


    @SuppressWarnings("unchecked")
    private <K extends CSTData,N extends CSTData> K findFirstZoneDataWithSourceFromHbase(Map map,
            IHBaseQueryAndPersistStrategy<K> ihBaseQueryAndPersistStrategy
            ,N needData) throws Exception {
        K temp=findHbaseZone(map, ihBaseQueryAndPersistStrategy,needData);
        log.debug("hbase zone data is {}",temp);
        return temp;

    }

    /**
     * 从hbse中获取区间第一条数据
     * @param map
     * @param ihBaseQueryAndPersistStrategy
     * @param needData
     * @return
     */
    private  <K extends CSTData,N extends CSTData> K findHbaseZone(Map map, IHBaseQueryAndPersistStrategy<K> ihBaseQueryAndPersistStrategy, N needData){
        log.debug("find first from hbase carid:{},time:{},timeselect:{},type:{}",needData.getCarId(), needData.getTime(),
                map.get(OtherKey.DataDealKey.TIME_SELECT),map.get(OtherKey.MIDLLE_DEAL.BESINESS_KEY_TYPE));
        map.put(OtherKey.DataDealKey.ROWKEY_GENERATE,
                new TimeSelectAndTypeRowKeyGrenerate<K>(needData.getCarId(), needData.getTime(),
                (CstConstants.TIME_SELECT) map.get(OtherKey.DataDealKey.TIME_SELECT),(String)map.get(OtherKey.MIDLLE_DEAL.BESINESS_KEY_TYPE)));
       return ihBaseQueryAndPersistStrategy.findHBaseData(map);
    }


    /**
     * 组合查找时区的第一条数据
     * @param map
     * @param ihBaseQueryAndPersistStrategy
     * @param firstZoneRedisKey
     * @param needData
     * @return
     * @throws Exception
     */
    private <K extends CSTData,N extends CSTData> K findFirstZoneData(Map map,
                                                                      IHBaseQueryAndPersistStrategy<K> ihBaseQueryAndPersistStrategy,
                                                                      String firstZoneRedisKey,N needData)throws Exception{
        K firstZoneData= findRedisData(firstZoneRedisKey);
        if(firstZoneData==null) {
            log.debug("find data from hbase {}",needData);
            firstZoneData = findFirstZoneDataWithSourceFromHbase(map, ihBaseQueryAndPersistStrategy, needData);

        }
       return firstZoneData;
    }

    /**
     * 数据存入缓存中
     * @return String
     * @param data
     */
    private <K extends CSTData> String firstDataToRedis(int expireTime,K data)  {
        String dataTmp = JsonHelper.toStringWithoutException(data);

        jedis.setex(firstZoneRedis,expireTime, dataTmp);
        log.debug("persist first data to redis is {}",dataTmp);
        return dataTmp;

    }

    /**
     * 数据持久化到hbase等
     * @param map
     * @param dataTmp
     */
    private void firstDataToPersist(Map map,String dataTmp){
        //下发到下一个bolt做持久化
        map.put(OtherKey.MIDLLE_DEAL.FIRST_TIME_ZONE, dataTmp);
        log.debug("first data to habse is {}",dataTmp);
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
     * 查询最近条数据，数据将永久在缓存中，这个是不设置过期时间的。
     * @param data
     * @param latestRedis
     * @throws Exception
     */
    private <K extends CSTData> void persistLatestDataToRedis(String latestRedis,K data)throws Exception{
        String dataTmp = JsonHelper.toStringWithoutException(data);
        jedis.set(latestRedis, dataTmp);
        log.debug("last data persist to redis is {}",dataTmp);
    }

    private <N extends CSTData> S findLastFirstZoneData(Map map,IHBaseQueryAndPersistStrategy<S> firstDataHbase,N needData ) throws Exception {

        //找上一次第一条数据
        String fmt = (String) map.get(OtherKey.MIDLLE_DEAL.FMT);
        String head = (String) map.get(OtherKey.MIDLLE_DEAL.REDIS_HEAD);
        //上一次的第一条数据redis key
        String lastFirstZoneKey = createFirstZoneRedisKey(head,fmt,needData);

        S firstDataAtLatestTimeSource=findFirstZoneData(map, firstDataHbase, lastFirstZoneKey, needData);
        if (firstDataAtLatestTimeSource == null) {
            log.info("This last first zone key is {},latestdata is {},upload data{}",lastFirstZoneKey,needData,s);
        }
        return firstDataAtLatestTimeSource;
    }

    public void dealHourData(Map map, IHBaseQueryAndPersistStrategy<S> firstDataHbase, IHBaseQueryAndPersistStrategy<T> resultDataQuery) throws Exception {
        dealData(map, firstDataHbase, resultDataQuery);
    }

    public void dealDayData(Map map, IHBaseQueryAndPersistStrategy<S> firstDataHbase, IHBaseQueryAndPersistStrategy<T> resultDataQuery) throws Exception {
        dealData(map, firstDataHbase, resultDataQuery);
    }

    public void dealData(Map map, IHBaseQueryAndPersistStrategy<S> firstDataHbase, IHBaseQueryAndPersistStrategy<T> resultDataQuery) throws Exception {

        //firstDataExpireTime
        int firstDataExpireTime = NumberUtils.toInt((String)map.get(OtherKey.MIDLLE_DEAL.ZONE_VALUE_EXPIRE_TIME),
                ZONE_VALUE_EXPIRE_TIME);
        log.debug("fist data expire time is {}", firstDataExpireTime);
        //当前上传数据的source 查找到的第一条数据
        S firstZoneData = findFirstZoneData(map, firstDataHbase,firstZoneRedis,s);
        //查找缓存中中间存储数据
        L latestData = findLatestDataFromRedis(latestRedis);

        //流程开始
        if (latestData == null) {
            log.debug("first data is :{}",s);
            //如果中间计算结果和持久化的区间第一条数据都是空,数据为第一条数据
            //初始化数据为中间结果，并存入到缓存中
            if (firstZoneData != null) {
                //缓存中没有查找到最近一条数据
                log.warn("data is exception,can not find latest data but found first zone data!latest key is {}",latestRedis);

            }else{
                //数据到缓存，并且持久化
                String dataTmp = firstDataToRedis(firstDataExpireTime, s);
                firstDataToPersist(map, dataTmp);
                //定时任务用的redis set
                scheduleDataToRedis(map, s);
            }
            latestData = c.initLatestData(s,map,null);
            log.debug("first latest data is :{}",latestData);

        }else{
            //判定数据是否异常,异常数据不入第一条数据，直接抛弃
            ExceptionCodeStatus status = c.commpareExceptionWithEachData(s, latestData, map);
            if (status == ExceptionCodeStatus.CALC_RETURN) {
                return;
            }
            //如果没有第一条数据，将该上传源数据作为第一条数据存入缓存和hbase中
            //数据跨区间处理
            if(firstZoneData==null){
                //数据到缓存，并且持久化
                String dataTmp = firstDataToRedis(firstDataExpireTime, s);
                firstDataToPersist(map, dataTmp);
                //定时任务用的redis set
                scheduleDataToRedis(map, s);
                //有可能该条数据没有第一条数据记录,但数据要累计到当前的最近数据里面
                log.debug("first latest data is :",latestData);
                if (status == ExceptionCodeStatus.CALC_NO_TIME) {
                    latestData=  c.calcLatestData(latestData, s,map,status);
                }else {
                    log.debug("cross data is latest data {},and source is {}:",latestData,s);
                    //上一次的第一条数据查找
                    S firstDataAtLatestTimeSource = findLastFirstZoneData(map, firstDataHbase, latestData);
                    log.debug("firstDataAtLatestTimeSource latest time is {}",firstDataAtLatestTimeSource);
                    if (firstDataAtLatestTimeSource != null) {
                        //数据区间值计算并补区间处理
                        persistIncreaseData(latestData, firstDataAtLatestTimeSource, map, resultDataQuery);
                    }
                    //最近数据用最新上传的数据初始化
                    latestData = c.initLatestData(s, map,latestData);
                }
            }else{
                //数据依然存在在该区间
                //只需要计算出最近的数据并放入缓存中
                latestData=  c.calcLatestData(latestData, s,map,status);
                if (status==ExceptionCodeStatus.CALC_JUMP) {
                    String dataTmp = firstDataToRedis(firstDataExpireTime, s);
                    firstDataToPersist(map, dataTmp);
                    scheduleDataToRedis(map, s);
                }

            }

            //缓存中存入最新计算的结果数据
        }
        persistLatestDataToRedis(latestRedis,latestData);
    }

    private void persistIncreaseData(L latestData,S latestFirstData,Map map,IHBaseQueryAndPersistStrategy<T> resultDataQuery) throws  ParseException {
        //比对区间，如果在同一区间,（在同一区间，只需要save到缓存，同时更新最近计算数据）
        long interval = (long) map.get(OtherKey.MIDLLE_DEAL.INTERVAL);
        String fmt = (String) map.get(OtherKey.MIDLLE_DEAL.FMT);
        T temp =  c.calcTransforData(latestFirstData, latestData, s, map);
        List<String> persistList=(LinkedList)map.get(OtherKey.MIDLLE_DEAL.PERSIST_KEY);
        if(persistList==null) {
            persistList = new LinkedList<>();
            map.put(OtherKey.MIDLLE_DEAL.PERSIST_KEY, persistList);
        }
        //数据跨区了，删除定时任务中的carId
       // removeScheduleDataRedis(map, latestFirstData);

        String dataTmp = JsonHelper.toStringWithoutException(temp);
        persistList.add(dataTmp);
        log.info("persist zone data to hbase is {}",dataTmp);

        List<String> nextList=(LinkedList)map.get(OtherKey.MIDLLE_DEAL.NEXT_KEY);
        if(nextList==null) {
            nextList = new LinkedList<>();
            map.put(OtherKey.MIDLLE_DEAL.NEXT_KEY, nextList);
        }
        nextList.add(dataTmp);
        log.debug("next zone data to hbase is {}",dataTmp);
        List<String> timeArea = DateTimeUtil.calcLongTimeBetween( latestFirstData.getTime(),s.getTime(), fmt, interval);
        if(CollectionUtils.isEmpty(timeArea)){
            return;
        }else if( timeArea.size() > 30 && timeArea.size() <=1000 )  {
            log.warn("Too much cross zone data: size={}, old: {}, latest data: {}, new: {}", timeArea.size(), latestFirstData, latestData, s);
        }if(timeArea.size() >1000 ){
            log.warn("Too much cross zone data ,can not do complement data: size={}, old: {}, latest data: {}, new: {}", timeArea.size(), latestFirstData, latestData, s);
        }

        //补齐之间的时间区域到持久化中
        for (String str : timeArea) {
            //以当前时区去获取数据，如果有则不做处理，如果没有则初始化
            map.put(OtherKey.DataDealKey.ROWKEY_GENERATE, new TimeSelectRowKeyGrenerate<T>(s.getCarId(), DateTimeUtil.strToTimestamp(str, fmt),
                    (CstConstants.TIME_SELECT) map.get(OtherKey.DataDealKey.TIME_SELECT)));
            //从hbse中获取区间数据
            T zoneStrategyHBaseData = resultDataQuery.findHBaseData(map);
            //仅仅需要判断第一个数据
            if(zoneStrategyHBaseData==null) {
                zoneStrategyHBaseData =  c.initFromTempTransfer(latestData, s ,map,DateTimeUtil.strToTimestamp(str, fmt));
                //仅仅补全区间数据，不纳入下次计算
                persistList.add(JsonHelper.toStringWithoutException(zoneStrategyHBaseData));
            }
            else {
                break;
            }

        }
    }

    /**
     * 单独用来处理定时任务的数据
     * @param map
     */
    public void executeScheduleData(Map map) throws Exception {
        if (StringUtils.isBlank(firstZoneRedis) || StringUtils.isBlank(latestRedis)) {
            log.info("can not create first or latest key {},{}",firstZoneRedis,latestRedis);
            return;
        }
        //获取第一条数据
        S firstZoneData= findRedisData(firstZoneRedis);
        if (firstZoneData == null) {
            log.info("schedule mission cannot find first data {}",firstZoneRedis);
            return;
        }
        L latestData = findLatestDataFromRedis(latestRedis);
        if (latestData == null) {
            log.info("schedule mission cannot find latest data {}",latestRedis);
            return;
        }
        //判断数据是否跨区间了
        String fmt = (String) map.get(OtherKey.MIDLLE_DEAL.FMT);
        boolean sameTimeZone=DateTimeUtil.dateTimeDifferentBetween(firstZoneData.getTime(), latestData.getTime(),fmt);
        if (!sameTimeZone) {
            log.info("schedule latest and first is different zone l:{} s:",latestData,firstZoneData);
            return;
        }
        T persistData = c.calcIntegrityData(firstZoneData, latestData,null, map);

        List<String> persistList=(LinkedList)map.get(OtherKey.MIDLLE_DEAL.PERSIST_KEY);
        if(persistList==null) {
            persistList = new LinkedList<>();
            map.put(OtherKey.MIDLLE_DEAL.PERSIST_KEY, persistList);
        }
        String dataTmp = JsonHelper.toStringWithoutException(persistData);
        persistList.add(dataTmp);
        log.info("schedule deal mission {}",dataTmp);

    }
    /**
     * 单独用来处理休眠包处理
     * @param map
     */
    public void executeDormancyData(Map map,CSTData source) throws Exception {
        //查找到当前日期下first数据有没有值，如果没有则初始化
        S firstZoneData = findRedisData(firstZoneRedis);
        if(firstZoneData!=null)
            return;
        L latestData = findLatestDataFromRedis(latestRedis);
        if (latestData == null) {
            log.info("dormancy mission cannot find latest data {}",latestRedis);
            return;
        }
        T persistData = c.initFromDormancy(latestData,source.getTime());
        List<String> persistList=(LinkedList)map.get(OtherKey.MIDLLE_DEAL.PERSIST_KEY);
        if(persistList==null) {
            persistList = new LinkedList<>();
            map.put(OtherKey.MIDLLE_DEAL.PERSIST_KEY, persistList);
        }
        String dataTmp = JsonHelper.toStringWithoutException(persistData);
        persistList.add(dataTmp);
        log.info("dormancy deal mission {}",dataTmp);
    }


    private  <N extends CSTData> void createFirstZoneRedis(String head, String fmt,N needData) {
        firstZoneRedis = createFirstZoneRedisKey(head, fmt, needData);
    }

    private   void createFirstZoneRedis(String head, String fmt,String carId,long time) {
        firstZoneRedis = createFirstZoneRedisKey(head, fmt, carId,time);
    }

    private <N extends CSTData>  String createFirstZoneRedisKey(String head, String fmt,N needData) {
        return StreamRedisConstants.StreamRedisFormat.getFirstZoneRedisKey(head, needData.getCarId(), DateTimeUtil.toLongTimeString(needData.getTime(), fmt));
    }
    private  String createFirstZoneRedisKey(String head, String fmt,String carId,long time) {
        return StreamRedisConstants.StreamRedisFormat.getFirstZoneRedisKey(head, carId, DateTimeUtil.toLongTimeString(time, fmt));
    }

    private <N extends CSTData>  void createResultRedisKey(String head, String fmt,N needData)throws NoSourceDataException{
        resultZoneRedis=StreamRedisConstants.StreamRedisFormat.getResultZoneRedisKey(head, needData.getCarId(), DateTimeUtil.toLongTimeString(needData.getTime(), fmt));
    }

    private <N extends CSTData>  void createLatestRedis(String head,N needData)throws NoSourceDataException {
        latestRedis = StreamRedisConstants.StreamRedisFormat.getLatestZoneRedisKey(head, needData.getCarId());
    }
    private  void createLatestRedis(String head,String carId)throws NoSourceDataException {
        latestRedis = StreamRedisConstants.StreamRedisFormat.getLatestZoneRedisKey(head, carId);
    }


    public GeneralDataStreamExecution createSpecialSource(S s) throws NoSourceDataException, IOException {
        if (s == null) {
            throw new NoSourceDataException("this data is empty");
        }
        this.setS(s);
        return this;
    }

    public GeneralDataStreamExecution createSpecialCalc(C c) {
        this.c = c;
        return this;
    }
    public GeneralDataStreamExecution createSpecialSource(String str) throws NoSourceDataException, IOException {
        if (StringUtils.isEmpty(str)) {
            throw new NoSourceDataException("this data is empty");
        }
        s = JsonHelper.toBeanWithoutException(str, new TypeReference<S>() {
        });
        return this;
    }
    public GeneralDataStreamExecution createSpecialSource(String str, String zone_key
            , String fmt) throws NoSourceDataException, IOException {
        if (StringUtils.isEmpty(str)) {
            throw new NoSourceDataException("this data is empty");
        }
        s = JsonHelper.toBeanWithoutException(str, new TypeReference<S>() {
        });
        createRedisKey(zone_key, fmt,s);
        return this;
    }

    /**
     * 创建所有redis key
     * @param zone_key
     * @param fmt
     * @param needData
     * @param <N>
     * @throws NoSourceDataException
     */
    public <N extends CSTData> void createRedisKey(String zone_key, String fmt,N needData) throws NoSourceDataException {
        try {
            createFirstZoneRedis(zone_key, fmt,needData);
            createResultRedisKey(zone_key, fmt,needData);
            createLatestRedis(zone_key,needData);
        }catch (NoSourceDataException e){
            throw e;
        }
    }

    /**
     * 创建所有redis key
     * @param zone_key
     * @param carId
     * @param time
     * @throws NoSourceDataException
     */
    public  void createRedisKey(String zone_key, String fmt, String carId,long time) throws NoSourceDataException {
        try {
            createFirstZoneRedis(zone_key, fmt,carId,time);
            createLatestRedis(zone_key,carId);
        }catch (NoSourceDataException e){
            throw e;
        }
    }

    public GeneralDataStreamExecution createIntegritySource(String str, String zone_key
            , String fmt) throws NoSourceDataException, IOException {
        if (StringUtils.isEmpty(str)) {
            throw new NoSourceDataException("this data is empty");
        }
        t = JsonHelper.toBeanWithoutException(str, new TypeReference<T>() {
        });
        /*s = JsonHelper.toBeanWithoutException(sourceStr, new TypeReference<S>() {
        });*/
        createRedisKey(zone_key, fmt,t);
        return this;
    }


    public GeneralDataStreamExecution createJedis(JedisCluster jedis) {
        this.jedis = jedis;
        return this;
    }

    public String gentMsgId(){
        return (s==null)?"default" : s.getCarId();
    }
    public String getSpecialSourceStr(S s) {
        return JsonHelper.toStringWithoutException(s);
    }


    public String getFirstZoneRedis() {
        return firstZoneRedis;
    }

    public void setFirstZoneRedis(String firstZoneRedis) {
        this.firstZoneRedis = firstZoneRedis;
    }

    public String getLatestRedis() {
        return latestRedis;
    }

    public void setLatestRedis(String latestRedis) {
        this.latestRedis = latestRedis;
    }

    public S getS() {
        return s;
    }

    public void setS(S s) {
        this.s = s;
    }

    public C getC() {
        return c;
    }

    public void setC(C c) {
        this.c = c;
    }

    public JedisCluster getJedis() {
        return jedis;
    }

    public void setJedis(JedisCluster jedis) {
        this.jedis = jedis;
    }

    /**
     * 计算数据完整性
     * @param map
     * @param firstDataHbase
     * @param resultDataQuery
     * @throws Exception
     */
    public Map<String,String> dealIntegrity(Map map, IHBaseQueryAndPersistStrategy<S> firstDataHbase, IHBaseQueryAndPersistStrategy<T> resultDataQuery)throws Exception{
        //当前上传数据的source 查找到的第一条数据
        log.info("firstZoneRedis:{} ,latestRedis:{}",firstZoneRedis,latestRedis);
        S firstZoneData = findFirstZoneData(map, firstDataHbase,firstZoneRedis,t);

        //查找缓存中中间存储数据
        L latestData = findLatestDataFromRedis(latestRedis);
        T temp = null;
        if(null!=firstZoneData && null!=latestData){
            temp =  c.calcIntegrityData(firstZoneData, latestData,t, map);
        }
        Map<String,String> dataMap = null;
        if(null!=latestData)
             dataMap = c.convertData2Map(temp,latestData);
        return dataMap;
    }


    /**
     * 将第一条数据carid存储到Set
     * @param carKeySetKey
     * @param carKeySetExpireTime
     * @param data
     */
    private void carIdAddRedisCarSet(String carKeySetKey, int carKeySetExpireTime, S data){
        if(jedis.exists(carKeySetKey)){
            jedis.sadd(carKeySetKey,data.getCarId());
            jedis.expire(carKeySetKey, carKeySetExpireTime);
        }else {
            jedis.sadd(carKeySetKey,data.getCarId());
        }
    }

    /**
     * 如果已经产生天结果,由Set删除
     * @param carKeySetKey
     * @param needData
     */
    private <N extends CSTData> void carIdDelRedisCarSet(String carKeySetKey, N needData){
        jedis.srem(carKeySetKey,needData.getCarId());
    }

    private  String createCarKeySetToRedisKey(String fmt,Long time) {
        return StreamRedisConstants.ZoneScheduleKey.getCarKeySetRedisKey(DateTimeUtil.toLongTimeString(time, fmt));
    }

    /**
     * 需要被定时任务处理的数据
     * @param map
     * @param s
     */
    private void scheduleDataToRedis(Map map,S s){
        if( (CstConstants.TIME_SELECT)map.get(OtherKey.DataDealKey.TIME_SELECT)==CstConstants.TIME_SELECT.DAY)
            carIdAddRedisCarSet(createCarKeySetToRedisKey((String) map.get(OtherKey.MIDLLE_DEAL.FMT),s.getTime())
                ,NumberUtils.toInt((String)map.get(OtherKey.MIDLLE_DEAL.ZONE_SCHEDULE_EXPIRE_TIME),
                        ZONE_SCHEDULE_DEALT_EXPIRE_TIME),s);
    }

    /**
     * 删除已经被计算过的数据
     * @param map
     * @param needData
     */
    private <N extends CSTData>void removeScheduleDataRedis(Map map,N needData){
        if( (CstConstants.TIME_SELECT)map.get(OtherKey.DataDealKey.TIME_SELECT)==CstConstants.TIME_SELECT.DAY)
            carIdDelRedisCarSet(createCarKeySetToRedisKey((String) map.get(OtherKey.MIDLLE_DEAL.FMT),needData.getTime()),needData);
    }
}

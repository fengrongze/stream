package cst.jstorm.daymonth.bolt.dispatch;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.cst.cmds.car.query.service.CarModelQueryService;
import com.cst.cmds.car.query.service.CarQueryService;
import com.cst.jstorm.commons.stream.constants.*;
import com.cst.jstorm.commons.stream.custom.ComsumerContextSelect;
import com.cst.jstorm.commons.stream.custom.CustomContextConfiguration;
import com.cst.jstorm.commons.stream.custom.GasProcess;
import com.cst.jstorm.commons.stream.custom.ProvinceProcess;
import com.cst.jstorm.commons.stream.operations.GeneralDataStreamExecution;
import com.cst.jstorm.commons.stream.operations.IntegratedDealTransforInterface;
import com.cst.jstorm.commons.stream.operations.hbasestrategy.IHBaseQueryAndPersistStrategy;
import com.cst.jstorm.commons.stream.operations.hbasestrategy.StrategyChoose;
import com.cst.jstorm.commons.stream.operations.hbasestrategy.TimeSelectAndTypeRowKeyGrenerate;
import com.cst.jstorm.commons.stream.operations.hbasestrategy.TimeSelectRowKeyGrenerate;
import com.cst.jstorm.commons.utils.*;
import com.cst.jstorm.commons.utils.exceptions.NoSourceDataException;
import com.cst.jstorm.commons.utils.http.HttpUtils;
import com.cst.jstorm.commons.utils.spring.MyApplicationContext;
import com.cst.stream.common.*;
import com.cst.stream.stathour.CSTData;
import com.cst.stream.stathour.am.*;
import com.cst.stream.stathour.de.*;
import com.cst.stream.stathour.integrated.DayIntegratedTransfor;
import com.cst.stream.stathour.obd.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.gson.Gson;
import cst.jstorm.daymonth.calcalations.am.AmDayCalcBiz;
import cst.jstorm.daymonth.calcalations.de.DeDayCalcBiz;
import cst.jstorm.daymonth.calcalations.integrated.DayIntegratedCalcBiz;
import cst.jstorm.daymonth.calcalations.obd.ObdDayCalcBiz;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 计算完整性,数据分发
 */
public class DispatchDayCalcBolt extends BaseBasicBolt {

    private static final long serialVersionUID = 5455826573425893424L;
    private transient Logger logger;
    private transient JedisCluster jedis;
    private Properties prop;
    private boolean forceLoad;
    private Producer<String, String> kafkaProducer;
    private String topic;
    private  static Gson gson = new Gson();
    private transient org.apache.hadoop.hbase.client.Connection connection;
    private AbstractApplicationContext beanContext;
    private transient CarQueryService carService;
    private transient CarModelQueryService modelService;
    /** 车辆所在城市缓存秒数 */
    private int CAR_CITY_CACHE_EXPIRE_SECONDS;
    /** 车辆油号缓存秒数 */
    private int CAR_GAS_NO_EXPIRE_SECONDS;
    /** 车辆油单价缓存秒数 */
    private int CAR_GAS_PRICE_EXPIRE_SECONDS;
    private transient HttpUtils httpUtils;
    private transient IntegratedDealTransforInterface integratedDealTransfor;
    private int DISPATCH_RECORD_EXPIRE;
    private  String DISPATCH_TYPE="1";
    public DispatchDayCalcBolt(Properties prop, boolean forceLoad) {
        this.prop = prop;
        this.forceLoad = forceLoad;
    }
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        prop = PropertiesUtil.initProp(prop, forceLoad);
        LogbackInitUtil.changeLogback(prop, true);
        jedis = RedisUtil.buildJedisCluster(prop, RedisKey.STORM_REDISCLUSTER);
        if (beanContext == null)
            beanContext = ComsumerContextSelect.getDefineContextWithHttpUtilWithParam(prop.getProperty("active.env"));
        logger = LoggerFactory.getLogger(DispatchDayCalcBolt.class);
        this.topic = prop.getProperty("kafka.stream.day.topic");
        this.kafkaProducer = KafkaProducerUtil.buildKafkaConsumer(prop);
        this.connection = (org.apache.hadoop.hbase.client.Connection) beanContext.getBean(OtherKey.DataDealKey.HBASE_CONNECTION);
        carService=(CarQueryService)beanContext.getBean("carQueryService");
        modelService = (CarModelQueryService) beanContext.getBean("modelQueryService");
        CAR_CITY_CACHE_EXPIRE_SECONDS = NumberUtils.toInt(prop.getProperty("city.expire.time"), ProvinceProcess.CITY_EXPIRE_TIME);
        CAR_GAS_NO_EXPIRE_SECONDS = NumberUtils.toInt(prop.getProperty("gas.num.expire.time"), RedisKey.ExpireTime.GAS_NUM_TIME);
        CAR_GAS_PRICE_EXPIRE_SECONDS = NumberUtils.toInt(prop.getProperty("gas.price.expire.time"), RedisKey.ExpireTime.GAS_PRICE_TIME);
        DISPATCH_RECORD_EXPIRE = NumberUtils.toInt(prop.getProperty("dispatch.day.record.expire.time"), RedisKey.ExpireTime.DISPATCH_DAY_RECORD_EXPIRE_TIME);
        httpUtils = (HttpUtils) beanContext.getBean(OtherKey.DataDealKey.HTTP_UTILS);
        integratedDealTransfor = new DayIntegratedCalcBiz();
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            String msg = input.getString(0);
            String msgId = input.getString(1);
            Integer itemValue = input.getInteger(2);
            logger.info("day dispatch receive msgId:{} ,itemValue:{}",msgId ,itemValue);
            if(itemValue==IntegrityDayCalcEnum.OBD.getValue()){
                dealIntegrity(msg);
            }
        } catch (Throwable e){
            logger.warn("计算天完整性数据出错",e);
        }
    }

    private void dealIntegrity(String msg) throws Exception{
        try {
            Map<String,String>  integrityMap = new HashMap<>();
            Map map=new HashMap<String,Object>(){{
                put(OtherKey.DataDealKey.TIME_SELECT, CstConstants.TIME_SELECT.DAY);
                put(OtherKey.MIDLLE_DEAL.FMT, DateTimeUtil.DEFAULT_DATE_DAY);
                put(OtherKey.MIDLLE_DEAL.INTERVAL, DateTimeUtil.ONE_DAY);
                put(OtherKey.MIDLLE_DEAL.ZONE_VALUE_EXPIRE_TIME, prop.getProperty("day.zone.value.expire.time"));

             }};
            ObdDayTransfor obdDayTransfor = JsonHelper.toBeanWithoutException(msg, new TypeReference<ObdDayTransfor>() {
            });
            //putFuelPerPrice(map, obdDayTransfor);

            dealObdIntegrity(msg, map,integrityMap);

            dealAmIntegrity(map ,obdDayTransfor.getCarId(),obdDayTransfor.getTime(),integrityMap);

            dealDeIntegrity(map ,obdDayTransfor.getCarId(),obdDayTransfor.getTime(),integrityMap);

            if(!integrityMap.isEmpty()){
                putCarIdAndTime(integrityMap, obdDayTransfor);
                dispatch(integrityMap);
            }else {
                logger.info("carid:{} ,time:{}完整天数据为空,不发送",obdDayTransfor.getCarId(),obdDayTransfor.getTime());
            }
        } catch (NoSourceDataException e) {
            e.printStackTrace();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            logger.error("obd data execute data is{}:", msg, e);
        } catch (IOException e) {
            logger.error("obd data execute data is{}:", msg, e);
        } catch (NullPointerException e) {
            logger.error("obd data execute  data is{}:", msg, e);
        } catch (Exception e) {
            logger.error("obd data execute  data is{}:", msg, e);
        }
       /* ObdDayTransfor obdDayTransfor = JsonHelper.toBeanWithoutException(msg, new TypeReference<ObdDayTransfor>() {
        });
        Map<String,String[]> familyQualifiers = createFamilyQualifiers();
        IHBaseQueryAndPersistStrategy<DayIntegratedTransfor> iDayIntegratedStrategy =
                StrategyChoose.generateStrategy(
                        connection, HBaseTable.DAY_STATISTICS.getTableName(),
                        familyQualifiers,
                        DayIntegratedTransfor.class);
        Map<String,Object> map = new HashMap<>();
        DayIntegratedTransfor dayIntegratedTransfor = findHbase(map,iDayIntegratedStrategy,obdDayTransfor);
        Map<String,String>  integrityMap = integratedDealTransfor.convertData2Map(dayIntegratedTransfor);

        if(!integrityMap.isEmpty()){
            putCarIdAndTime(integrityMap, obdDayTransfor);
            dispatch(integrityMap);
        }else {
            logger.info("carid:{} ,time:{}完整天数据为空,不发送",obdDayTransfor.getCarId(),obdDayTransfor.getTime());
        }*/
    }

    /**
     * 加入油价
     * @param map
     * @param obdHourTransfor
     */
    private void putFuelPerPrice(Map map, ObdDayTransfor obdHourTransfor) {
        String  fuelPerPrice= null ;
        if (!Boolean.valueOf(prop.getProperty("ignore.outside.status"))) {
            fuelPerPrice = String.valueOf(GasProcess.getCarGasPrice(
                    prop.getProperty("url_base").concat(HttpURIUtil.CAR_PRICE_URL),
                    jedis, obdHourTransfor.getCarId(), obdHourTransfor.getTime(),
                    DateTimeUtil.toLongTimeString(obdHourTransfor.getTime(), DateTimeUtil.DEFAULT_DATE_DEFULT)
                    ,
                    httpUtils, CAR_GAS_PRICE_EXPIRE_SECONDS
            ));

        }
        map.put("fuelPrice", StringUtils.isBlank(fuelPerPrice)?prop.getProperty("default_oil_price"):fuelPerPrice);
    }

    private  void dealObdIntegrity(String msg, Map map,Map<String,String>  integrityMap) throws NoSourceDataException, Exception {
        map.put(OtherKey.MIDLLE_DEAL.BESINESS_KEY_TYPE,StreamTypeDefine.OBD_TYPE);
        GeneralDataStreamExecution<ObdHourSource, ObdDayTransfor, ObdDayLatestData,ObdDayCalcBiz> generalStreamExecution = new GeneralDataStreamExecution<>()
                .createJedis(jedis)
                .createSpecialCalc(new ObdDayCalcBiz())
                .createIntegritySource(msg, StreamRedisConstants.DayKey.DAY_OBD, DateTimeUtil.DEFAULT_DATE_DAY);
        IHBaseQueryAndPersistStrategy<ObdHourSource> iFirstStrategy =
                StrategyChoose.generateStrategy(
                        connection, HBaseTable.DAY_FIRST_ZONE.getTableName(),
                        HBaseTable.DAY_FIRST_ZONE.getFirstFamilyName(), HbaseColumn.DaySourceColumn.obdDayColumns,
                        ObdHourSource.class);

        Map<String,String> obdMap = generalStreamExecution.dealIntegrity(map,iFirstStrategy, null);
        if (null!=obdMap && !obdMap.isEmpty()){
            integrityMap.putAll(obdMap);
        }
    }

    private  void dealAmIntegrity(Map map,String carid,Long time,Map<String,String>  integrityMap) throws NoSourceDataException, Exception {
        map.put(OtherKey.MIDLLE_DEAL.BESINESS_KEY_TYPE,StreamTypeDefine.AM_TYPE);
        AmDayTransfor amDayTransfor = new AmDayTransfor();
        amDayTransfor.setTime(time);
        amDayTransfor.setCarId(carid);
        String msg = JsonHelper.toStringWithoutException(amDayTransfor);

        GeneralDataStreamExecution<AmHourSource, AmDayTransfor, AmDayLatestData,AmDayCalcBiz> generalStreamExecution = new GeneralDataStreamExecution<>()
                .createJedis(jedis)
                .createSpecialCalc(new AmDayCalcBiz())
                .createIntegritySource(msg,StreamRedisConstants.DayKey.DAY_AM, DateTimeUtil.DEFAULT_DATE_DAY);
        IHBaseQueryAndPersistStrategy<AmHourSource> iFirstStrategy =
                StrategyChoose.generateStrategy(
                        connection, HBaseTable.DAY_FIRST_ZONE.getTableName(),
                        HBaseTable.DAY_FIRST_ZONE.getFirstFamilyName(), HbaseColumn.DaySourceColumn.amDayColumns,
                        AmHourSource.class);

        Map<String,String> amMap = generalStreamExecution.dealIntegrity(map,iFirstStrategy, null);

        if (null!=amMap && !amMap.isEmpty()){
            integrityMap.putAll(amMap);
        }
    }

    private void   dealDeIntegrity(Map map,String carid,Long time,Map<String,String>  integrityMap) throws NoSourceDataException, Exception {
        map.put(OtherKey.MIDLLE_DEAL.BESINESS_KEY_TYPE,StreamTypeDefine.DE_TYPE);
        DeDayTransfor deDayTransfor = new DeDayTransfor();
        deDayTransfor.setTime(time);
        deDayTransfor.setCarId(carid);
        String msg = JsonHelper.toStringWithoutException(deDayTransfor);

        GeneralDataStreamExecution<DeHourSource, DeDayTransfor, DeDayLatestData,DeDayCalcBiz> generalStreamExecution = new GeneralDataStreamExecution<>()
                .createJedis(jedis)
                .createSpecialCalc(new DeDayCalcBiz())
                .createIntegritySource(msg, StreamRedisConstants.DayKey.DAY_DE, DateTimeUtil.DEFAULT_DATE_DAY);
        IHBaseQueryAndPersistStrategy<DeHourSource> iFirstStrategy =
                StrategyChoose.generateStrategy(
                        connection, HBaseTable.DAY_FIRST_ZONE.getTableName(),
                        HBaseTable.DAY_FIRST_ZONE.getFirstFamilyName(), HbaseColumn.DaySourceColumn.deDayColumns,
                        DeHourSource.class);

        Map<String,String> deMap = generalStreamExecution.dealIntegrity(map,iFirstStrategy, null);

        if (null!=deMap && !deMap.isEmpty()){
            integrityMap.putAll(deMap);
        }
    }
    private  <K extends CSTData,N extends CSTData> K findHbase(Map map, IHBaseQueryAndPersistStrategy<K> ihBaseQueryAndPersistStrategy, N needData){
        logger.debug("find first from hbase carid:{},time:{},timeselect:{},type:{}",needData.getCarId(), needData.getTime(),
                map.get(OtherKey.DataDealKey.TIME_SELECT),map.get(OtherKey.MIDLLE_DEAL.BESINESS_KEY_TYPE));
        CstConstants.TIME_SELECT timeSelect= CstConstants.TIME_SELECT.DAY;
        TimeSelectRowKeyGrenerate timeSelectRowKeyGrenerate = new TimeSelectRowKeyGrenerate(needData.getCarId(),
                needData.getTime(),
                timeSelect);
        map.put(OtherKey.DataDealKey.ROWKEY_GENERATE, timeSelectRowKeyGrenerate);
        return ihBaseQueryAndPersistStrategy.findHBaseData(map);
    }

    private void putCarIdAndTime(Map<String, String> integrityMap, ObdDayTransfor obdDayTransfor) {
            integrityMap.put("time",String.valueOf(obdDayTransfor.getTime()));
            integrityMap.put("carId",obdDayTransfor.getCarId());
            integrityMap.put("din",obdDayTransfor.getDin());
    }

    private void dispatch(Map<String,String> currentData) {
        if(null!=currentData && !currentData.isEmpty()){
            String carId = currentData.get("carId");
            if(isNotRepeatedRecord(currentData)) {
                currentData.put("type", DISPATCH_TYPE);
                String sendData = gson.toJson(currentData);
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(this.topic, carId, sendData);
                logger.info("carid:{} ,time:{}发送完整天数据", currentData.get("carId"), currentData.get("time"));
                kafkaProducer.send(record, new Callback() {
                    //@Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (metadata != null) {
                            logger.debug("渠道发送数据成功送达!topic:{},partition:{},offset:{}", metadata.topic(), metadata.partition(),
                                    metadata.offset());
                        }
                        if (exception != null) {
                            logger.warn("car:{} 天完整数据发送kafka异常:", carId, exception);
                        }
                    }
                });
            }else {
                logger.info("carid:{} ,time:{}推送消息重复,不发送kafka",carId,currentData.get("time"));
            }
        }
    }

    /**
     * 重复消息判断
     * @param currentData
     * @return
     */
    private boolean isNotRepeatedRecord(Map<String,String> currentData) {
        boolean isNotRepeatedRecord = true;
        String key = StreamRedisConstants.DispatchRecord.getDispatchRecordKey(DISPATCH_TYPE,currentData.get("carId"),currentData.get("time"));
        if(jedis.setnx(key, "")<=0){
            isNotRepeatedRecord = false;
        }else{
            jedis.expire(key, DISPATCH_RECORD_EXPIRE);
        }
        return isNotRepeatedRecord;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void cleanup() {
        super.cleanup();
    }
}

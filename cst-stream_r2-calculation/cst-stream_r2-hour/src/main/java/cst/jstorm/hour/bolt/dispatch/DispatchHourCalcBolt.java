package cst.jstorm.hour.bolt.dispatch;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.cst.cmds.car.query.service.CarModelQueryService;
import com.cst.cmds.car.query.service.CarQueryService;
import com.cst.jstorm.commons.stream.constants.IntegrityHourCalcEnum;
import com.cst.jstorm.commons.stream.constants.OtherKey;
import com.cst.jstorm.commons.stream.custom.ComsumerContextSelect;
import com.cst.jstorm.commons.stream.custom.CustomContextConfiguration;
import com.cst.jstorm.commons.stream.custom.GasProcess;
import com.cst.jstorm.commons.stream.custom.ProvinceProcess;
import com.cst.jstorm.commons.stream.operations.GeneralDataStreamExecution;
import com.cst.jstorm.commons.stream.operations.IntegratedDealTransforInterface;
import com.cst.jstorm.commons.stream.operations.hbasestrategy.IHBaseQueryAndPersistStrategy;
import com.cst.jstorm.commons.stream.operations.hbasestrategy.StrategyChoose;
import com.cst.jstorm.commons.stream.operations.hbasestrategy.TimeSelectRowKeyGrenerate;
import com.cst.jstorm.commons.utils.*;
import com.cst.jstorm.commons.utils.exceptions.NoSourceDataException;
import com.cst.jstorm.commons.utils.http.HttpUtils;
import com.cst.jstorm.commons.utils.spring.MyApplicationContext;
import com.cst.stream.common.*;
import com.cst.stream.stathour.CSTData;
import com.cst.stream.stathour.am.*;
import com.cst.stream.stathour.de.*;
import com.cst.stream.stathour.obd.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.gson.Gson;
import com.cst.jstorm.commons.stream.constants.RedisKey;
import cst.jstorm.hour.calcalations.am.AmHourCalcBiz;
import cst.jstorm.hour.calcalations.de.DeHourCalcBiz;
import cst.jstorm.hour.calcalations.integrated.HourIntegratedCalcBiz;
import cst.jstorm.hour.calcalations.obd.ObdHourCalcBiz;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


/**
 * 计算完整性,小时数据分发
 */
public class DispatchHourCalcBolt extends BaseBasicBolt {

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
    /** 车辆油单价缓存秒数 */
    private int CAR_GAS_PRICE_EXPIRE_SECONDS;
    private int DISPATCH_RECORD_EXPIRE;
    private transient HttpUtils httpUtils;
    private transient IntegratedDealTransforInterface integratedDealTransfor;
    private String DISPATCH_TYPE="0";
    public DispatchHourCalcBolt(Properties prop, boolean forceLoad) {
        this.prop = prop;
        this.forceLoad = forceLoad;
    }
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        prop = PropertiesUtil.initProp(prop, forceLoad);
        LogbackInitUtil.changeLogback(prop, true);
        jedis = RedisUtil.buildJedisCluster(prop, RedisKey.STORM_REDISCLUSTER);
        logger = LoggerFactory.getLogger(DispatchHourCalcBolt.class);
        this.topic = prop.getProperty("kafka.stream.hour.topic");
        this.kafkaProducer = KafkaProducerUtil.buildKafkaConsumer(prop);
        if (beanContext == null)
            beanContext = ComsumerContextSelect.getDefineContextWithHttpUtilWithParam(prop.getProperty("active.env"));
        this.connection = (org.apache.hadoop.hbase.client.Connection) beanContext.getBean(OtherKey.DataDealKey.HBASE_CONNECTION);

        CAR_GAS_PRICE_EXPIRE_SECONDS = NumberUtils.toInt(prop.getProperty("gas.price.expire.time"), RedisKey.ExpireTime.GAS_PRICE_TIME);
        DISPATCH_RECORD_EXPIRE = NumberUtils.toInt(prop.getProperty("dispatch.hour.record.expire.time"), RedisKey.ExpireTime.DISPATCH_HOUR_RECORD_EXPIRE_TIME);
        httpUtils = (HttpUtils) beanContext.getBean(OtherKey.DataDealKey.HTTP_UTILS);
        this.integratedDealTransfor = new HourIntegratedCalcBiz();
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            String msg = input.getString(0);
            String msgId = input.getString(1);
            Integer itemValue = input.getInteger(2);
            logger.info("hour dispatch receive msgId:{} ,itemValue:{}",msgId ,itemValue);
            if(itemValue==IntegrityHourCalcEnum.OBD.getValue()){
                dealIntegrity(msg);
            }
        } catch (Throwable e){
            logger.warn("计算小时完整性数据出错",e);
        }
    }

    private void dealIntegrity(String msg) throws Exception{
        try {
            Map<String,String>  integrityMap = new HashMap<>();

            //计算补充


            Map map=new HashMap<String,Object>(){{
                put(OtherKey.DataDealKey.TIME_SELECT, CstConstants.TIME_SELECT.HOUR);
                put(OtherKey.MIDLLE_DEAL.FMT, DateTimeUtil.DEFAULT_DATE_HOUR);
                put(OtherKey.MIDLLE_DEAL.INTERVAL, DateTimeUtil.ONE_HOUR);
                put(OtherKey.MIDLLE_DEAL.LAST_VALUE_EXPIRE_TIME, prop.getProperty("last.value.expire.time"));
                put(OtherKey.MIDLLE_DEAL.ZONE_VALUE_EXPIRE_TIME, prop.getProperty("hour.zone.value.expire.time"));
                //
            }};
            ObdHourTransfor obdHourTransfor = JsonHelper.toBeanWithoutException(msg, new TypeReference<ObdHourTransfor>() {

            });

            //putFuelPerPrice(map, obdHourTransfor);

            dealObdIntegrity(msg, map,integrityMap);

            dealAmIntegrity(map ,obdHourTransfor.getCarId(),obdHourTransfor.getTime(),integrityMap);

            dealDeIntegrity(map ,obdHourTransfor.getCarId(),obdHourTransfor.getTime(),integrityMap);

            if(!integrityMap.isEmpty()){
                putCarIdAndTime(integrityMap, obdHourTransfor);
                dispatch(integrityMap);
            }else {
                logger.info("carid:{} ,time:{}完整小时数据为空,不发送",obdHourTransfor.getCarId(),obdHourTransfor.getTime());
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

      /*  ObdHourTransfor obdHourTransfor = JsonHelper.toBeanWithoutException(msg, new TypeReference<ObdHourTransfor>() {
        });
        Map<String,String[]> familyQualifiers = createFamilyQualifiers();
        IHBaseQueryAndPersistStrategy<HourIntegratedTransfor> iHourIntegratedStrategy =
                StrategyChoose.generateStrategy(
                        connection, HBaseTable.HOUR_STATISTICS.getTableName(),
                        familyQualifiers,
                        HourIntegratedTransfor.class);
        Map<String,Object> map = new HashMap<>();
        HourIntegratedTransfor hourIntegratedTransfor = findHbase(map,iHourIntegratedStrategy,obdHourTransfor);
        Map<String,String>  integrityMap = integratedDealTransfor.convertData2Map(hourIntegratedTransfor);

        if(!integrityMap.isEmpty()){
            putCarIdAndTime(integrityMap, obdHourTransfor);
            dispatch(integrityMap);
        }else {
            logger.info("carid:{} ,time:{}完整小时数据为空,不发送",obdHourTransfor.getCarId(),obdHourTransfor.getTime());
        }*/
    }

    /**
     * 加入油价
     * @param map
     * @param obdHourTransfor
     */
    private void putFuelPerPrice(Map map, ObdHourTransfor obdHourTransfor) {
        String  fuelPerPrice= null ;
        if (!Boolean.valueOf(prop.getProperty("ignore.outside.status"))) {
            fuelPerPrice = String.valueOf(GasProcess.getCarGasPrice(
                    prop.getProperty("url_base").concat(HttpURIUtil.CAR_PRICE_URL),
                    jedis, obdHourTransfor.getCarId(), obdHourTransfor.getTime(),
                    DateTimeUtil.toLongTimeString(obdHourTransfor.getTime(), DateTimeUtil.DEFAULT_DATE_DEFULT)
                    , httpUtils, CAR_GAS_PRICE_EXPIRE_SECONDS
            ));

        }
        map.put("fuelPrice", StringUtils.isBlank(fuelPerPrice)?prop.getProperty("default_oil_price"):fuelPerPrice);
    }

    private  void dealObdIntegrity(String msg, Map map,Map<String,String>  integrityMap) throws NoSourceDataException, Exception {
        map.put(OtherKey.MIDLLE_DEAL.BESINESS_KEY_TYPE,StreamTypeDefine.OBD_TYPE);
        GeneralDataStreamExecution<ObdHourSource, ObdHourTransfor, ObdHourLatestData,ObdHourCalcBiz> generalStreamExecution = new GeneralDataStreamExecution<>()
                .createJedis(jedis)
                .createSpecialCalc(new ObdHourCalcBiz())
                .createIntegritySource(msg, StreamRedisConstants.HourKey.HOUR_OBD, DateTimeUtil.DEFAULT_DATE_HOUR);
        IHBaseQueryAndPersistStrategy<ObdHourSource> iFirstStrategy =
                StrategyChoose.generateStrategy(
                        connection, HBaseTable.HOUR_FIRST_ZONE.getTableName(),
                        HBaseTable.HOUR_FIRST_ZONE.getFirstFamilyName(), HbaseColumn.HourSourceColumn.obdHourColumns,
                        ObdHourSource.class);

        Map<String,String> obdMap = generalStreamExecution.dealIntegrity(map,iFirstStrategy, null);
        if (null!=obdMap && !obdMap.isEmpty()){
            integrityMap.putAll(obdMap);
        }
    }

    private  void dealAmIntegrity(Map map,String carid,Long time,Map<String,String>  integrityMap) throws NoSourceDataException, Exception {
        map.put(OtherKey.MIDLLE_DEAL.BESINESS_KEY_TYPE,StreamTypeDefine.AM_TYPE);
        AmHourTransfor amHourTransfor = new AmHourTransfor();
        amHourTransfor.setTime(time);
        amHourTransfor.setCarId(carid);
        String msg = JsonHelper.toStringWithoutException(amHourTransfor);

        GeneralDataStreamExecution<AmHourSource, AmHourTransfor, AmHourLatestData,AmHourCalcBiz> generalStreamExecution = new GeneralDataStreamExecution<>()
                .createJedis(jedis)
                .createSpecialCalc(new AmHourCalcBiz())
                .createIntegritySource(msg ,StreamRedisConstants.HourKey.HOUR_AM, DateTimeUtil.DEFAULT_DATE_HOUR);
        IHBaseQueryAndPersistStrategy<AmHourSource> iFirstStrategy =
                StrategyChoose.generateStrategy(
                        connection, HBaseTable.HOUR_FIRST_ZONE.getTableName(),
                        HBaseTable.HOUR_FIRST_ZONE.getFirstFamilyName(), HbaseColumn.HourSourceColumn.amHourColumns,
                        AmHourSource.class);

        Map<String,String> amMap = generalStreamExecution.dealIntegrity(map,iFirstStrategy, null);

        if (null!=amMap && !amMap.isEmpty()){
            integrityMap.putAll(amMap);
        }
    }

    private void   dealDeIntegrity(Map map,String carid,Long time,Map<String,String>  integrityMap) throws NoSourceDataException, Exception {
        map.put(OtherKey.MIDLLE_DEAL.BESINESS_KEY_TYPE,StreamTypeDefine.DE_TYPE);
        DeHourTransfor deHourTransfor = new DeHourTransfor();
        deHourTransfor.setTime(time);
        deHourTransfor.setCarId(carid);
        String msg = JsonHelper.toStringWithoutException(deHourTransfor);

        GeneralDataStreamExecution<DeHourSource, DeHourTransfor, DeHourLatestData,DeHourCalcBiz> generalStreamExecution = new GeneralDataStreamExecution<>()
                .createJedis(jedis)
                .createSpecialCalc(new DeHourCalcBiz())
                .createIntegritySource(msg,StreamRedisConstants.HourKey.HOUR_DE, DateTimeUtil.DEFAULT_DATE_HOUR);
        IHBaseQueryAndPersistStrategy<DeHourSource> iFirstStrategy =
                StrategyChoose.generateStrategy(
                        connection, HBaseTable.HOUR_FIRST_ZONE.getTableName(),
                        HBaseTable.HOUR_FIRST_ZONE.getFirstFamilyName(), HbaseColumn.HourSourceColumn.deHourColumns,
                        DeHourSource.class);

        Map<String,String> deMap = generalStreamExecution.dealIntegrity(map,iFirstStrategy, null);

        if (null!=deMap && !deMap.isEmpty()){
            integrityMap.putAll(deMap);
        }
    }
    private static Map<String,String[]> createFamilyQualifiers() {
        Map<String,String[]> familyQualifiers = new HashMap<>();
        familyQualifiers.put(HBaseTable.HOUR_STATISTICS.getFirstFamilyName(),HbaseColumn.HourStatisticsCloumn.obdHourColumns);
        familyQualifiers.put(HBaseTable.HOUR_STATISTICS.getFourthFamilyName(),HbaseColumn.HourStatisticsCloumn.deHourColumns);
        return familyQualifiers;
    }

    private  <K extends CSTData,N extends CSTData> K findHbase(Map map, IHBaseQueryAndPersistStrategy<K> ihBaseQueryAndPersistStrategy, N needData){
        logger.debug("find first from hbase carid:{},time:{},timeselect:{},type:{}",needData.getCarId(), needData.getTime(),
                map.get(OtherKey.DataDealKey.TIME_SELECT),map.get(OtherKey.MIDLLE_DEAL.BESINESS_KEY_TYPE));
        CstConstants.TIME_SELECT timeSelect= CstConstants.TIME_SELECT.HOUR;
        TimeSelectRowKeyGrenerate timeSelectRowKeyGrenerate = new TimeSelectRowKeyGrenerate(needData.getCarId(),
                needData.getTime(),
                timeSelect);
        map.put(OtherKey.DataDealKey.ROWKEY_GENERATE, timeSelectRowKeyGrenerate);
        return ihBaseQueryAndPersistStrategy.findHBaseData(map);
    }

    /**
     * 将carid和time加入
     */
    private void putCarIdAndTime(Map<String, String> integrityMap, ObdHourTransfor obdHourTransfor) {
        integrityMap.put("time",String.valueOf(obdHourTransfor.getTime()));
        integrityMap.put("carId",obdHourTransfor.getCarId());
        integrityMap.put("din",obdHourTransfor.getDin());
    }

    private void dispatch(Map<String,String> currentData) {
        if(null!=currentData && !currentData.isEmpty()){
            String carId = currentData.get("carId");
            if(isNotRepeatedRecord(currentData)){
                currentData.put("type",DISPATCH_TYPE);
                String sendData = gson.toJson(currentData);
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(this.topic, carId,sendData);
                logger.info("carid:{} ,发送完整小时数据:{}",currentData.get("carId"),sendData);
                kafkaProducer.send(record, new Callback() {
                    //@Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (metadata != null) {
                            logger.debug("渠道发送小时数据成功送达!topic:{},partition:{},offset:{}",metadata.topic(), metadata.partition(),
                                    metadata.offset());
                        }
                        if (exception != null) {
                            logger.warn("car:{} 小时完整数据发送kafka异常:",carId,exception);
                        }
                    }
                });
            } else {
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

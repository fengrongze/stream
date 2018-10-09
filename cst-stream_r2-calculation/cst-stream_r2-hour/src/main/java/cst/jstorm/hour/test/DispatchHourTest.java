/*
package cst.jstorm.hour.test;

import backtype.storm.Config;
import com.cst.cmds.car.query.service.CarModelQueryService;
import com.cst.cmds.car.query.service.CarQueryService;
import com.cst.config.client.ConfigClient;
import com.cst.jstorm.commons.stream.constants.OtherKey;
import com.cst.jstorm.commons.stream.constants.RedisKey;
import com.cst.jstorm.commons.stream.custom.GasProcess;
import com.cst.jstorm.commons.stream.custom.ProvinceProcess;
import com.cst.jstorm.commons.stream.operations.GeneralDataStreamExecution;
import com.cst.jstorm.commons.stream.operations.hbasestrategy.IHBaseQueryAndPersistStrategy;
import com.cst.jstorm.commons.stream.operations.hbasestrategy.StrategyChoose;
import com.cst.jstorm.commons.utils.HttpURIUtil;
import com.cst.jstorm.commons.utils.PropertiesUtil;
import com.cst.jstorm.commons.utils.RedisUtil;
import com.cst.jstorm.commons.utils.exceptions.NoSourceDataException;
import com.cst.jstorm.commons.utils.spring.MyApplicationContext;
import com.cst.stream.common.*;
import com.cst.stream.stathour.am.AmHourLatestData;
import com.cst.stream.stathour.am.AmHourSource;
import com.cst.stream.stathour.am.AmHourTransfor;
import com.cst.stream.stathour.de.DeHourLatestData;
import com.cst.stream.stathour.de.DeHourSource;
import com.cst.stream.stathour.de.DeHourTransfor;
import com.cst.stream.stathour.obd.ObdHourLatestData;
import com.cst.stream.stathour.obd.ObdHourSource;
import com.cst.stream.stathour.obd.ObdHourTransfor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.gson.Gson;
import cst.jstorm.hour.calcalations.am.AmHourCalcBiz;
import cst.jstorm.hour.calcalations.de.DeHourCalcBiz;
import cst.jstorm.hour.calcalations.obd.ObdHourCalcBiz;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;
import java.util.*;

public class DispatchHourTest {
    private static Logger logger = LoggerFactory.getLogger(DispatchHourTest.class);
    private static JedisCluster jedis;
    private static org.apache.hadoop.hbase.client.Connection connection;
    private static Properties prop;
    private  static Gson gson = new Gson();
    private static transient CarQueryService carService;
    private static transient CarModelQueryService modelService;
    */
/** 车辆所在城市缓存秒数 *//*

    private static int CAR_CITY_CACHE_EXPIRE_SECONDS;
    */
/** 车辆油号缓存秒数 *//*

    private static int CAR_GAS_NO_EXPIRE_SECONDS;
    */
/** 车辆油单价缓存秒数 *//*

    private static int CAR_GAS_PRICE_EXPIRE_SECONDS;
    public static void main(String[] args) {
        DispatchHourTest tddt = new DispatchHourTest();
        ObdHourTransfor tdhs = new ObdHourTransfor();
        tdhs.setCarId("M201855553347");
        tdhs.setTime(1533785704000L);
        String msg = JsonHelper.toStringWithoutException(tdhs);
        logger.debug("trace delete hour transfor data:{}", msg);
         prop = tddt.loadProp("config.properties");
         jedis = RedisUtil.buildJedisCluster(prop, RedisKey.STORM_REDISCLUSTER);
        AbstractApplicationContext beanContext = MyApplicationContext.getDefaultContext();
        connection = (org.apache.hadoop.hbase.client.Connection) beanContext.getBean(OtherKey.DataDealKey.HBASE_CONNECTION);
        //carService=(CarQueryService)beanContext.getBean("carQueryService");
        //modelService = (CarModelQueryService) beanContext.getBean("modelQueryService");
        CAR_CITY_CACHE_EXPIRE_SECONDS = NumberUtils.toInt(prop.getProperty("city.expire.time"), ProvinceProcess.CITY_EXPIRE_TIME);
        CAR_GAS_NO_EXPIRE_SECONDS = NumberUtils.toInt(prop.getProperty("gas.num.expire.time"), RedisKey.ExpireTime.GAS_NUM_TIME);
        CAR_GAS_PRICE_EXPIRE_SECONDS = NumberUtils.toInt(prop.getProperty("gas.price.expire.time"), RedisKey.ExpireTime.GAS_PRICE_TIME);
        try {
            tddt.dealIntegrity(msg );

        } catch (Exception e) {
            logger.error("execute trace delete day persist no source  data is{}:", msg, e);

        }
    }

    private void dealIntegrity(String msg ) throws Exception{
        try {
            Map<String,String>  integrityMap = new HashMap<>();
            Map map=new HashMap<String,Object>(){{
                put(OtherKey.DataDealKey.TIME_SELECT, CstConstants.TIME_SELECT.HOUR);
                put(OtherKey.MIDLLE_DEAL.FMT, DateTimeUtil.DEFAULT_DATE_HOUR);
                put(OtherKey.MIDLLE_DEAL.INTERVAL, DateTimeUtil.ONE_HOUR);
                put(OtherKey.MIDLLE_DEAL.LAST_VALUE_EXPIRE_TIME, prop.getProperty("last.value.expire.time"));
                put(OtherKey.MIDLLE_DEAL.ZONE_VALUE_EXPIRE_TIME, prop.getProperty("hour.zone.value.expire.time"));

            }};
            ObdHourTransfor obdHourTransfor = JsonHelper.toBeanWithoutException(msg, new TypeReference<ObdHourTransfor>() {

            });

            putFuelPerPrice(map, obdHourTransfor);

            dealObdIntegrity(msg, map,integrityMap);

            //dealAmIntegrity(map ,obdHourTransfor.getCarId(),obdHourTransfor.getTime(),integrityMap);

            //dealDeIntegrity(map ,obdHourTransfor.getCarId(),obdHourTransfor.getTime(),integrityMap);

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
        logger.info("am小时统计:{}",gson.toJson(amMap));
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
    */
/**
     * 将carid和time加入
     *//*

    private void putCarIdAndTime(Map<String, String> integrityMap, ObdHourTransfor obdHourTransfor) {
        if(null==integrityMap.get("time")){
            integrityMap.put("time",String.valueOf(obdHourTransfor.getTime()));
        }
        if(null==integrityMap.get("carId")){
            integrityMap.put("carId",obdHourTransfor.getCarId());
        }
    }
    private void dispatch(Map<String,String> currentData) {
        if(null!=currentData && !currentData.isEmpty()){
            String carId = currentData.get("carId");
            String sendData = gson.toJson(currentData);
            //ProducerRecord<String, String> record = new ProducerRecord<String, String>(this.topic, carId,sendData);
            logger.info("carid:{} ,发送完整小时数据:{}",currentData.get("carId"),sendData);
        }
    }

    protected Properties loadProp(String localConfigName) {
        Properties prop = this.getLocalProperties(localConfigName);
        ConfigClient config = new ConfigClient();
        this.enrichConfigClient(config, prop);
        config.start();
        config.loadProperties(prop);
        config.stop();
        return prop;
    }

    public Properties getLocalProperties(String localConfigName) {
        Properties properties = new Properties();

        try {
            properties.load(PropertiesUtil.class.getClassLoader().getResourceAsStream(localConfigName));
        } catch (Exception var4) {
            logger.error("properties load failure", var4);
        }

        return properties;
    }

    public void enrichConfigClient(ConfigClient config, Properties properties) {
        config.setZookeeperUrl(properties.getProperty("zookeeper.url"));
        config.setLevels(properties.getProperty("levels"));
        config.setConfigFilename(properties.getProperty("config.filename"));
    }

    public Config initConf(Properties props, String param) {
        Config conf = new Config();
        return conf;
    }

    //protected abstract TopologyBuilder createBuilder(Properties var1, String var2);

    protected String getName(String pre, String param) {
        if (param.contains("obd")) {
            return pre + "_obd";
        } else if (param.contains("gps")) {
            return pre + "_gps";
        } else {
            return param.contains("integrated") ? pre + "_integrated" : pre;
        }
    }

    */
/**
     * 加入油价
     * @param map
     * @param obdHourTransfor
     *//*

    private void putFuelPerPrice(Map map, ObdHourTransfor obdHourTransfor) {
        String  fuelPerPrice= null ;
        if (!Boolean.valueOf(prop.getProperty("ignore.outside.status"))) {
            fuelPerPrice = String.valueOf(GasProcess.getCarGasPrice(
                    prop.getProperty("url_base").concat(HttpURIUtil.CAR_PRICE_URL),
                    jedis, obdHourTransfor.getCarId(), obdHourTransfor.getTime(),
                    DateTimeUtil.toLongTimeString(obdHourTransfor.getTime(), DateTimeUtil.DEFAULT_DATE_DEFULT)
                    , carService, modelService,
                    null, CAR_GAS_NO_EXPIRE_SECONDS, CAR_GAS_PRICE_EXPIRE_SECONDS, CAR_CITY_CACHE_EXPIRE_SECONDS
            ));

        }
        map.put("fuelPrice", StringUtils.isBlank(fuelPerPrice)?prop.getProperty("default_oil_price"):fuelPerPrice);
    }
}
*/

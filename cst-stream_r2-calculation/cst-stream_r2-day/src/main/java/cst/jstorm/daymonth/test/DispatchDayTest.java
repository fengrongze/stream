/*
package cst.jstorm.daymonth.test;

import backtype.storm.Config;
import com.cst.config.client.ConfigClient;
import com.cst.jstorm.commons.stream.constants.OtherKey;
import com.cst.jstorm.commons.stream.constants.RedisKey;
import com.cst.jstorm.commons.stream.operations.GeneralDataStreamExecution;
import com.cst.jstorm.commons.stream.operations.hbasestrategy.IHBaseQueryAndPersistStrategy;
import com.cst.jstorm.commons.stream.operations.hbasestrategy.StrategyChoose;
import com.cst.jstorm.commons.utils.PropertiesUtil;
import com.cst.jstorm.commons.utils.RedisUtil;
import com.cst.jstorm.commons.utils.exceptions.NoSourceDataException;
import com.cst.jstorm.commons.utils.spring.MyApplicationContext;
import com.cst.stream.common.*;
import com.cst.stream.stathour.am.*;
import com.cst.stream.stathour.de.*;
import com.cst.stream.stathour.obd.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.gson.Gson;
import cst.jstorm.daymonth.calcalations.am.AmDayCalcBiz;
import cst.jstorm.daymonth.calcalations.de.DeDayCalcBiz;
import cst.jstorm.daymonth.calcalations.obd.ObdDayCalcBiz;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class DispatchDayTest {
    private static Logger logger = LoggerFactory.getLogger(DispatchDayTest.class);
    private static JedisCluster jedis;
    private static org.apache.hadoop.hbase.client.Connection connection;
    private static Properties prop;
    private  static Gson gson = new Gson();
    public static void main(String[] args) {
        DispatchDayTest tddt = new DispatchDayTest();
        ObdDayTransfor tdhs = new ObdDayTransfor();
        tdhs.setCarId("M201600010008");
        tdhs.setTime(1534338433000L);
        String msg = JsonHelper.toStringWithoutException(tdhs);
        logger.debug("trace delete day transfor data:{}", msg);
         prop = tddt.loadProp("config.properties");
         jedis = RedisUtil.buildJedisCluster(prop, RedisKey.STORM_REDISCLUSTER);
        AbstractApplicationContext beanContext = MyApplicationContext.getDefaultContext();
         connection = (org.apache.hadoop.hbase.client.Connection) beanContext.getBean(OtherKey.DataDealKey.HBASE_CONNECTION);
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
                put(OtherKey.DataDealKey.TIME_SELECT, CstConstants.TIME_SELECT.DAY);
                put(OtherKey.MIDLLE_DEAL.FMT, DateTimeUtil.DEFAULT_DATE_DAY);
                put(OtherKey.MIDLLE_DEAL.INTERVAL, DateTimeUtil.ONE_DAY);
                put(OtherKey.MIDLLE_DEAL.LAST_VALUE_EXPIRE_TIME, prop.getProperty("last.value.expire.time"));
                put(OtherKey.MIDLLE_DEAL.ZONE_VALUE_EXPIRE_TIME, prop.getProperty("day.zone.value.expire.time"));

            }};
            ObdDayTransfor obdDayTransfor = JsonHelper.toBeanWithoutException(msg, new TypeReference<ObdDayTransfor>() {

            });
            dealObdIntegrity(msg, map,integrityMap);

            dealAmIntegrity(map ,obdDayTransfor.getCarId(),obdDayTransfor.getTime(),integrityMap);

            dealDeIntegrity(map ,obdDayTransfor.getCarId(),obdDayTransfor.getTime(),integrityMap);

            if(!integrityMap.isEmpty()){
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

    }

    private  void dealObdIntegrity(String msg, Map map,Map<String,String>  integrityMap) throws NoSourceDataException, Exception {
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

        AmDayTransfor amDayTransfor = new AmDayTransfor();
        amDayTransfor.setTime(time);
        amDayTransfor.setCarId(carid);
        String msg = JsonHelper.toStringWithoutException(amDayTransfor);

        GeneralDataStreamExecution<AmHourSource, AmDayTransfor, AmDayLatestData,AmDayCalcBiz> generalStreamExecution = new GeneralDataStreamExecution<>()
                .createJedis(jedis)
                .createSpecialCalc(new AmDayCalcBiz())
                .createIntegritySource(msg ,StreamRedisConstants.DayKey.DAY_AM, DateTimeUtil.DEFAULT_DATE_DAY);
        IHBaseQueryAndPersistStrategy<AmHourSource> iFirstStrategy =
                StrategyChoose.generateStrategy(
                        connection, HBaseTable.DAY_FIRST_ZONE.getTableName(),
                        HBaseTable.DAY_FIRST_ZONE.getFirstFamilyName(), HbaseColumn.DaySourceColumn.amDayColumns,
                        AmHourSource.class);

        Map<String,String> amMap = generalStreamExecution.dealIntegrity(map,iFirstStrategy, null);
        logger.info("am小时统计:{}",gson.toJson(amMap));
        if (null!=amMap && !amMap.isEmpty()){
            integrityMap.putAll(amMap);
        }
    }

    private void   dealDeIntegrity(Map map,String carid,Long time,Map<String,String>  integrityMap) throws NoSourceDataException, Exception {

        DeDayTransfor deDayTransfor = new DeDayTransfor();
        deDayTransfor.setTime(time);
        deDayTransfor.setCarId(carid);
        String msg = JsonHelper.toStringWithoutException(deDayTransfor);

        GeneralDataStreamExecution<DeHourSource, DeDayTransfor, DeDayLatestData,DeDayCalcBiz> generalStreamExecution = new GeneralDataStreamExecution<>()
                .createJedis(jedis)
                .createSpecialCalc(new DeDayCalcBiz())
                .createIntegritySource(msg,StreamRedisConstants.DayKey.DAY_DE, DateTimeUtil.DEFAULT_DATE_DAY);
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

}
*/

package cst.jstorm.hour.test;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.cst.cmds.car.query.service.CarModelQueryService;
import com.cst.cmds.car.query.service.CarQueryService;
import com.cst.jstorm.commons.stream.constants.OtherKey;
import com.cst.jstorm.commons.stream.constants.PropKey;
import com.cst.jstorm.commons.stream.constants.RedisKey;
import com.cst.jstorm.commons.stream.constants.StreamKey;
import com.cst.jstorm.commons.stream.custom.CustomContextConfiguration;
import com.cst.jstorm.commons.stream.custom.GasProcess;
import com.cst.jstorm.commons.stream.custom.ProvinceProcess;
import com.cst.jstorm.commons.stream.operations.GeneralDataStreamExecution;
import com.cst.jstorm.commons.stream.operations.hbasestrategy.IHBaseQueryAndPersistStrategy;
import com.cst.jstorm.commons.stream.operations.hbasestrategy.StrategyChoose;
import com.cst.jstorm.commons.utils.HttpURIUtil;
import com.cst.jstorm.commons.utils.RedisUtil;
import com.cst.jstorm.commons.utils.http.HttpUtils;
import com.cst.jstorm.commons.utils.spring.MyApplicationContext;
import com.cst.stream.common.*;
import com.cst.stream.stathour.obd.*;
import cst.jstorm.hour.calcalations.obd.ObdHourCalcBiz;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import redis.clients.jedis.JedisCluster;

import java.util.*;

public class ObdHourDataCalcTest {
    private transient Logger logger = LoggerFactory.getLogger(ObdHourDataCalcTest.class);
    private static JedisCluster jedis;
    private static AbstractApplicationContext beanContext;
    private static org.apache.hadoop.hbase.client.Connection connection;
    private static HttpUtils httpUtils;
    private static CarQueryService carService;
    private static CarModelQueryService modelService;

    /** 车辆所在城市缓存秒数 */
    private static int CAR_CITY_CACHE_EXPIRE_SECONDS;
    /** 车辆油号缓存秒数 */
    private static int CAR_GAS_NO_EXPIRE_SECONDS;
    /** 车辆油单价缓存秒数 */
    private static int CAR_GAS_PRICE_EXPIRE_SECONDS;
    private static Properties prop;
    private static ObdHourCalcBiz obdHourCalcBiz;
    public static void main(String[] args) {
        //beanContext = MyApplicationContext.getDefaultContext();
        beanContext = MyApplicationContext.getDefineContextWithHttpUtil(CustomContextConfiguration.class);
        //logger.info("----------------------------------------beanContext is {}", beanContext);
        prop = new Properties();
        prop.setProperty("storm.rediscluster","172.16.131.58:7000,172.16.131.58:7001,172.16.131.58:7002,172.16.131.58:7003,172.16.131.58:7004");
        prop.setProperty("city.expire.time","172800");
        prop.setProperty("gas.num.expire.time","172800");
        prop.setProperty("gas.price.expire.time","86400");
        prop.setProperty("last.value.expire.time","86400");
        prop.setProperty("hour.zone.value.expire.time","7200");
        prop.setProperty("ignore.outside.status","false");
        prop.setProperty("url_base","http://172.16.133.82:8088/");
        prop.setProperty("default_oil_price","7.8");
        prop.setProperty("deal_strategy","hbase_client");
        prop.setProperty("highSpeedStandard","100");
        jedis = RedisUtil.buildJedisCluster(prop, RedisKey.STORM_REDISCLUSTER);

        connection = (org.apache.hadoop.hbase.client.Connection) beanContext.getBean(OtherKey.DataDealKey.HBASE_CONNECTION);
        httpUtils = (HttpUtils) beanContext.getBean(OtherKey.DataDealKey.HTTP_UTILS);
        carService=(CarQueryService)beanContext.getBean("carQueryService");
        modelService = (CarModelQueryService) beanContext.getBean("modelQueryService");

        CAR_CITY_CACHE_EXPIRE_SECONDS = NumberUtils.toInt(prop.getProperty("city.expire.time"), ProvinceProcess.CITY_EXPIRE_TIME);
        CAR_GAS_NO_EXPIRE_SECONDS = NumberUtils.toInt(prop.getProperty("gas.num.expire.time"), RedisKey.ExpireTime.GAS_NUM_TIME);
        CAR_GAS_PRICE_EXPIRE_SECONDS = NumberUtils.toInt(prop.getProperty("gas.price.expire.time"), RedisKey.ExpireTime.GAS_PRICE_TIME);
        obdHourCalcBiz = new ObdHourCalcBiz();
        ObdHourDataCalcTest obdHourDataCalcTest = new ObdHourDataCalcTest();
        ObdHourSource obdHourSource = obdHourDataCalcTest.createObdHourSource();
        obdHourDataCalcTest.execute(JsonHelper.toStringWithoutException(obdHourSource));

    }


    public void execute(String msg) {
        if (StringUtils.isEmpty(msg)) {
            logger.info("msg is empty,return ");
            return;
        }

        try {
            Map<String, Object> map = new HashMap<String, Object>() {{
                put(OtherKey.DataDealKey.TIME_SELECT, CstConstants.TIME_SELECT.HOUR);
                put(OtherKey.MIDLLE_DEAL.NEXT_KEY, new LinkedList<String>());
                //put(OtherKey.MIDLLE_DEAL.NEXT_STREAM, StreamKey.ObdStream.OBD_DAY_BOLT_F);
                put(OtherKey.MIDLLE_DEAL.FMT, DateTimeUtil.DEFAULT_DATE_HOUR);
                put(OtherKey.MIDLLE_DEAL.INTERVAL, DateTimeUtil.ONE_HOUR);
                put(OtherKey.MIDLLE_DEAL.LAST_DATA_PARAM, StreamTypeDefine.OBD_TYPE);
                put(OtherKey.MIDLLE_DEAL.LAST_VALUE_EXPIRE_TIME, "86400");
                put(OtherKey.MIDLLE_DEAL.ZONE_VALUE_EXPIRE_TIME, "86400");
                put(OtherKey.MIDLLE_DEAL.BESINESS_KEY_TYPE,StreamTypeDefine.OBD_TYPE);
                put(OtherKey.MIDLLE_DEAL.REDIS_HEAD, StreamRedisConstants.HourKey.HOUR_OBD);

            }};
            GeneralDataStreamExecution<ObdHourSource, ObdHourTransfor,ObdHourLatestData, ObdHourCalcBiz> generalStreamExecution =
                    new GeneralDataStreamExecution<>()
                            .createJedis(jedis)
                            .createSpecialCalc(obdHourCalcBiz)
                            .createSpecialSource(msg, StreamRedisConstants.HourKey.HOUR_OBD, DateTimeUtil.DEFAULT_DATE_HOUR);
            // 获取车辆的油单价
            String fuelPerPrice=null;
            if (!Boolean.valueOf(prop.getProperty("ignore.outside.status"))) {
                fuelPerPrice = String.valueOf(GasProcess.getCarGasPrice(
                        prop.getProperty("url_base").concat(HttpURIUtil.CAR_PRICE_URL),
                        jedis, generalStreamExecution.getS().getCarId(), generalStreamExecution.getS().getTime(),
                        DateTimeUtil.toLongTimeString(generalStreamExecution.getS().getTime(), DateTimeUtil.DEFAULT_DATE_DEFULT)
                        ,
                        httpUtils, CAR_GAS_PRICE_EXPIRE_SECONDS
                ));

            }
            //计算补充
            map.put("fuelPrice", StringUtils.isBlank(fuelPerPrice)?prop.getProperty("default_oil_price"):fuelPerPrice);
            map.put("highSpeedStandard", prop.getProperty("highSpeedStandard"));
            map.put("night_high", prop.getProperty("night_high"));
            map.put("night_low", prop.getProperty("night_low"));
            map.put("travel_speed", prop.getProperty("travel_speed"));
            map.put("jump_mile", prop.getProperty("jump_mile"));
            map.put("tooling_probability_count", prop.getProperty("tooling_probability_count"));
            IHBaseQueryAndPersistStrategy<ObdHourSource> iFirstStrategy = StrategyChoose.generateStrategy(prop.getProperty(PropKey.DEAL_STRATEGY),
                    httpUtils, prop.getProperty("url_base"), HttpURIUtil.OBD_HOUR_SOURCE_FIND,
                    connection, HBaseTable.HOUR_FIRST_ZONE.getTableName(),
                    HBaseTable.HOUR_FIRST_ZONE.getFirstFamilyName(), HbaseColumn.HourSourceColumn.obdHourColumns,
                    ObdHourSource.class);
            IHBaseQueryAndPersistStrategy<ObdHourTransfor> iResultStrategy = StrategyChoose.generateStrategy(prop.getProperty(PropKey.DEAL_STRATEGY),
                    httpUtils, prop.getProperty("url_base"), HttpURIUtil.OBD_HOUR_FIND,
                    connection, HBaseTable.HOUR_STATISTICS.getTableName(),
                    HBaseTable.HOUR_STATISTICS.getFirstFamilyName(), HbaseColumn.HourStatisticsCloumn.obdHourColumns,
                    ObdHourTransfor.class);

            logger.debug("deal hour data :{} ",msg);
            generalStreamExecution.dealHourData(map,iFirstStrategy, iResultStrategy);

            //hbase中拿不到该时区数据 查找最近一条上传的数据

            List<String> persistValues = (List) map.get(OtherKey.MIDLLE_DEAL.PERSIST_KEY);
            if (CollectionUtils.isNotEmpty(persistValues)) {
                for (String str : persistValues) {
                    logger.info("结果数据存储:{}",str);
                    //collector.emit(StreamKey.ObdStream.OBD_HOUR_BOLT_S, new Values(str, generalStreamExecution.gentMsgId()));
                }
            }
            if (map.get(OtherKey.MIDLLE_DEAL.FIRST_TIME_ZONE) != null) {
                logger.info("FIRST_TIME_ZONE数据存储:{}",map.get(OtherKey.MIDLLE_DEAL.FIRST_TIME_ZONE));
                /*collector.emit(StreamKey.ObdStream.OBD_HOUR_BOLT_FIRST_DATA,
                        new Values(map.get(OtherKey.MIDLLE_DEAL.FIRST_TIME_ZONE),
                                generalStreamExecution.gentMsgId()));*/

            }
            logger.info("计算完成....................");
        } catch (Throwable e) {
            logger.error("Process OBD hour data exception: {}", msg, e);
        }
    }

    private ObdHourSource createObdHourSource(){

       /* ObdHourSource obdHourSource = new ObdHourSource("M1234325439594", 0, 1100F,
                120.01F, 1100, 0F,80)
                .carId("yx0011100009991").time(1534474898000L);*/
        return null;
    }
}

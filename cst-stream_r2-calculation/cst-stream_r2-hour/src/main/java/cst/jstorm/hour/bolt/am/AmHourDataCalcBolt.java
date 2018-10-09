package cst.jstorm.hour.bolt.am;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.cst.cmds.car.query.service.CarModelQueryService;
import com.cst.cmds.car.query.service.CarQueryService;
import com.cst.jstorm.commons.stream.custom.ComsumerContextSelect;
import com.cst.jstorm.commons.stream.custom.CustomContextConfiguration;
import com.cst.stream.common.*;
import com.cst.stream.stathour.am.AmHourLatestData;
import com.cst.stream.stathour.am.AmHourSource;
import com.cst.stream.stathour.am.AmHourTransfor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.cst.jstorm.commons.stream.constants.OtherKey;
import com.cst.jstorm.commons.stream.constants.PropKey;
import com.cst.jstorm.commons.stream.constants.RedisKey;
import com.cst.jstorm.commons.stream.constants.StreamKey;
import com.cst.jstorm.commons.stream.custom.ProvinceProcess;
import com.cst.jstorm.commons.stream.operations.GeneralDataStreamExecution;
import com.cst.jstorm.commons.stream.operations.hbasestrategy.IHBaseQueryAndPersistStrategy;
import com.cst.jstorm.commons.stream.operations.hbasestrategy.StrategyChoose;
import com.cst.jstorm.commons.utils.HttpURIUtil;
import com.cst.jstorm.commons.utils.LogbackInitUtil;
import com.cst.jstorm.commons.utils.PropertiesUtil;
import com.cst.jstorm.commons.utils.RedisUtil;
import com.cst.jstorm.commons.utils.exceptions.NoSourceDataException;
import com.cst.jstorm.commons.utils.http.HttpUtils;
import com.cst.jstorm.commons.utils.spring.MyApplicationContext;
import cst.jstorm.hour.calcalations.am.AmHourCalcBiz;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;
import java.text.ParseException;
import java.util.*;

/**
 * @author Johnney.chiu
 * create on 2017/11/24 18:25
 * @Description AM data calc
 */
public class AmHourDataCalcBolt extends BaseBasicBolt {
    private static final long serialVersionUID = -2378660439755159092L;
    private transient Logger logger;
    private AbstractApplicationContext beanContext;
    private transient JedisCluster jedis;
    private Properties prop;
    private boolean forceLoad;
    private transient org.apache.hadoop.hbase.client.Connection connection;
    private transient HttpUtils httpUtils;
    private transient CarQueryService carService;
    private transient CarModelQueryService modelService;
    /** 车辆所在城市缓存秒数 */
    private int CAR_CITY_CACHE_EXPIRE_SECONDS;
    /** 车辆油号缓存秒数 */
    private int CAR_GAS_NO_EXPIRE_SECONDS;
    /** 百度AK */
    private String BAIDU_AK;
    /** 百度SK */
    private String BAIDU_SK;

    private AmHourCalcBiz amHourCalcBiz;

    public AmHourDataCalcBolt(Properties prop, boolean forceLoad) {
        this.prop = prop;
        this.forceLoad = forceLoad;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void execute(Tuple tuple,BasicOutputCollector collector) {
        if(!StreamKey.AmStream.AM_HOUR_BOLT_F.equals(tuple.getSourceStreamId()))
            return;
        String msg = tuple.getString(0);
        if(StringUtils.isEmpty(msg))
            return;
        try {
            logger.debug("am hour source data:{}",msg);
            Map<String, Object> map = new HashMap<String,Object>(){{
                put(OtherKey.DataDealKey.TIME_SELECT, CstConstants.TIME_SELECT.HOUR);
                put(OtherKey.MIDLLE_DEAL.NEXT_KEY, new LinkedList<String>());
                put(OtherKey.MIDLLE_DEAL.FMT, DateTimeUtil.DEFAULT_DATE_HOUR);
                put(OtherKey.MIDLLE_DEAL.LAST_DATA_PARAM, StreamTypeDefine.AM_TYPE);
                put(OtherKey.MIDLLE_DEAL.INTERVAL, DateTimeUtil.ONE_HOUR);
                put(OtherKey.MIDLLE_DEAL.BESINESS_KEY_TYPE,StreamTypeDefine.AM_TYPE);
                put(OtherKey.MIDLLE_DEAL.REDIS_HEAD, StreamRedisConstants.HourKey.HOUR_AM);
                put(OtherKey.MIDLLE_DEAL.ZONE_VALUE_EXPIRE_TIME, prop.getProperty("hour.zone.value.expire.time"));
            }};
            GeneralDataStreamExecution<AmHourSource, AmHourTransfor,AmHourLatestData, AmHourCalcBiz> generalStreamExecution =
                    new GeneralDataStreamExecution<>()
                    .createJedis(jedis)
                    .createSpecialCalc(amHourCalcBiz)
                    .createSpecialSource(msg, StreamRedisConstants.HourKey.HOUR_AM,
                            DateTimeUtil.DEFAULT_DATE_HOUR);
            //根据经纬度算出地方并存储在缓存中
            if(!Boolean.valueOf(prop.getProperty("ignore.outside.status"))&&AlarmTypeConst.ALARM_CAR_IGNITION==generalStreamExecution.getS().getAlarmType().intValue())
                ProvinceProcess.getCityFromLalo(jedis, generalStreamExecution.getS().getCarId(),
                        CAR_CITY_CACHE_EXPIRE_SECONDS,
                        httpUtils, generalStreamExecution.getS().getLatitude(),
                        generalStreamExecution.getS().getLongitude(),
                        BAIDU_AK, BAIDU_SK,carService,modelService,CAR_GAS_NO_EXPIRE_SECONDS);

            IHBaseQueryAndPersistStrategy<AmHourSource> iFirstZoneStrategy = StrategyChoose.generateStrategy(prop.getProperty(PropKey.DEAL_STRATEGY),
                    httpUtils, prop.getProperty("url_base"), HttpURIUtil.AM_HOUR_SOURCE_FIND,
                    connection, HBaseTable.HOUR_FIRST_ZONE.getTableName(),
                    HBaseTable.HOUR_FIRST_ZONE.getFirstFamilyName(), HbaseColumn.HourSourceColumn.amHourColumns,
                    AmHourSource.class);

            IHBaseQueryAndPersistStrategy<AmHourTransfor> iResultStrategy = StrategyChoose.generateStrategy(prop.getProperty(PropKey.DEAL_STRATEGY),
                    httpUtils, prop.getProperty("url_base"), HttpURIUtil.AM_HOUR_FIND,
                    connection, HBaseTable.HOUR_STATISTICS.getTableName(),
                    HBaseTable.HOUR_STATISTICS.getThirdFamilyName(), HbaseColumn.HourStatisticsCloumn.amHourColumns,
                    AmHourTransfor.class);
            generalStreamExecution.dealHourData(map,iFirstZoneStrategy,iResultStrategy);


            List<String> persistValues = (List) map.get(OtherKey.MIDLLE_DEAL.PERSIST_KEY);
            if(CollectionUtils.isNotEmpty(persistValues))
                for(String str:persistValues) {
                    logger.debug("##persist am hour data is {},list is {}",str,persistValues.toString());
                    collector.emit(StreamKey.AmStream.AM_HOUR_BOLT_S, new Values(str, generalStreamExecution.gentMsgId()));
                }

            if (map.get(OtherKey.MIDLLE_DEAL.FIRST_TIME_ZONE) != null) {
                logger.debug("first time data:{}",map.get(OtherKey.MIDLLE_DEAL.FIRST_TIME_ZONE));
                collector.emit(StreamKey.AmStream.AM_HOUR_BOLT_FIRST_DATA,
                        new Values(map.get(OtherKey.MIDLLE_DEAL.FIRST_TIME_ZONE),
                                generalStreamExecution.gentMsgId()));
            }

        }catch (NoSourceDataException e) {
            logger.error("am hour calc no source data is{}:",msg, e);

        } catch (JsonProcessingException e) {
            logger.error("am hour calc data is{}:",msg, e);
        } catch (ParseException e) {
            logger.error("am hour calc  data is{}:",msg, e);
        } catch (IOException e) {
            logger.error("am hour calc data is{}:",msg, e);
        }catch (NullPointerException e) {
            logger.error("am hour calc data is{}:",msg, e);
        }catch (Exception e){
            logger.error("am hour calc  data is{}:",msg, e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(StreamKey.AmStream.AM_HOUR_BOLT_S, new Fields(new String[] {
                StreamKey.AmStream.AM_KEY_F, StreamKey.AmStream.AM_KEY_S}));
        outputFieldsDeclarer.declareStream(StreamKey.AmStream.AM_HOUR_BOLT_FIRST_DATA, new Fields(new String[] {
                StreamKey.AmStream.AM_KEY_F, StreamKey.AmStream.AM_KEY_S}));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf,context);
        prop = PropertiesUtil.initProp(prop, forceLoad);
        LogbackInitUtil.changeLogback(prop,true);
        jedis = RedisUtil.buildJedisCluster(prop, RedisKey.STORM_REDISCLUSTER);
        beanContext = ComsumerContextSelect.getDefineContextWithHttpUtilWithParam(prop.getProperty("active.env"));
        logger = LoggerFactory.getLogger(AmHourDataCalcBolt.class);
        connection = (org.apache.hadoop.hbase.client.Connection) beanContext.getBean(OtherKey.DataDealKey.HBASE_CONNECTION);
        httpUtils = (HttpUtils) beanContext.getBean(OtherKey.DataDealKey.HTTP_UTILS);
        BAIDU_AK = prop.getProperty("baidu.api.ak");
        BAIDU_SK = prop.getProperty("baidu.api.sk");
        CAR_CITY_CACHE_EXPIRE_SECONDS = NumberUtils.toInt(prop.getProperty("city.expire.time"), ProvinceProcess.CITY_EXPIRE_TIME);
        CAR_GAS_NO_EXPIRE_SECONDS = NumberUtils.toInt(prop.getProperty("gas.num.expire.time"), RedisKey.ExpireTime.GAS_NUM_TIME);
        carService=(CarQueryService)beanContext.getBean("carQueryService");
        modelService = (CarModelQueryService) beanContext.getBean("modelQueryService");
        amHourCalcBiz = new AmHourCalcBiz();

    }

    @Override
    public void cleanup() {
        super.cleanup();
        if(beanContext!=null)
            beanContext.close();
    }
}

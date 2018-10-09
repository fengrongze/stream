package cst.jstorm.hour.bolt.gps;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.cst.stream.common.*;
import com.cst.stream.stathour.gps.GpsHourLatestData;
import com.cst.stream.stathour.gps.GpsHourSource;
import com.cst.stream.stathour.gps.GpsHourTransfor;
import com.cst.jstorm.commons.stream.constants.OtherKey;
import com.cst.jstorm.commons.stream.constants.PropKey;
import com.cst.jstorm.commons.stream.constants.RedisKey;
import com.cst.jstorm.commons.stream.constants.StreamKey;
import com.cst.jstorm.commons.stream.operations.GeneralDataStreamExecution;
import com.cst.jstorm.commons.stream.operations.hbasestrategy.IHBaseQueryAndPersistStrategy;
import com.cst.jstorm.commons.stream.operations.hbasestrategy.StrategyChoose;
import com.cst.jstorm.commons.utils.HttpURIUtil;
import com.cst.jstorm.commons.utils.LogbackInitUtil;
import com.cst.jstorm.commons.utils.PropertiesUtil;
import com.cst.jstorm.commons.utils.RedisUtil;
import com.cst.jstorm.commons.utils.http.HttpUtils;
import com.cst.jstorm.commons.utils.spring.MyApplicationContext;
import cst.jstorm.hour.calcalations.gps.GpsHourCalcBiz;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import redis.clients.jedis.JedisCluster;

import java.util.*;

/**
 * @author Johnney.chiu
 * create on 2017/11/21 10:01
 * @Description gps 的数据小时处理
 */
public class GpsHourDataCalcBolt extends BaseBasicBolt {
    private static final int EXTIRE_TIME = 2 * 60 * 60;
    private static final long serialVersionUID = -8567375293634725465L;
    private transient Logger logger;
    private transient JedisCluster jedis;
    private AbstractApplicationContext beanContext;
    private Properties prop;
    private boolean forceLoad;
    private transient org.apache.hadoop.hbase.client.Connection connection;
    private transient HttpUtils httpUtils;
    private GpsHourCalcBiz gpsHourCalcBiz;


    public GpsHourDataCalcBolt(Properties prop, boolean forceLoad) {
        this.prop = prop;
        this.forceLoad = forceLoad;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf,context);
        prop = PropertiesUtil.initProp(prop, forceLoad);
        LogbackInitUtil.changeLogback(prop,true);
        beanContext = MyApplicationContext.getDefaultContext();
        logger = LoggerFactory.getLogger(GpsHourDataCalcBolt.class);
        jedis = RedisUtil.buildJedisCluster(prop, RedisKey.STORM_REDISCLUSTER);
        connection = (org.apache.hadoop.hbase.client.Connection) beanContext.getBean(OtherKey.DataDealKey.HBASE_CONNECTION);
        httpUtils = (HttpUtils) beanContext.getBean(OtherKey.DataDealKey.HTTP_UTILS);
        gpsHourCalcBiz = new GpsHourCalcBiz();

    }

    @Override
    public void cleanup() {
        super.cleanup();
        if (beanContext != null)
            beanContext.close();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        if (!StreamKey.GpsStream.GPS_HOUR_BOLT_F.equals(tuple.getSourceStreamId())) {
            return;
        }
        String msg = tuple.getString(0);
        if (StringUtils.isEmpty(msg)) {
            return;
        }
        try {
            logger.debug("gps hour source data:{}",msg);
            Map<String, Object> map = new HashMap<String,Object>(){{
                put(OtherKey.DataDealKey.TIME_SELECT, CstConstants.TIME_SELECT.HOUR);
                put(OtherKey.MIDLLE_DEAL.NEXT_KEY, new LinkedList<String>());
                //put(OtherKey.MIDLLE_DEAL.NEXT_STREAM, StreamKey.GpsStream.GPS_DAY_BOLT_F);
                put(OtherKey.MIDLLE_DEAL.FMT, DateTimeUtil.DEFAULT_DATE_HOUR);
                put(OtherKey.MIDLLE_DEAL.INTERVAL, DateTimeUtil.ONE_HOUR);
                put(OtherKey.MIDLLE_DEAL.LAST_DATA_PARAM, StreamTypeDefine.GPS_TYPE);
                put(OtherKey.MIDLLE_DEAL.LAST_VALUE_EXPIRE_TIME, prop.getProperty("last.value.expire.time"));
                put(OtherKey.MIDLLE_DEAL.ZONE_VALUE_EXPIRE_TIME, prop.getProperty("hour.zone.value.expire.time"));
                put(OtherKey.MIDLLE_DEAL.BESINESS_KEY_TYPE,StreamTypeDefine.GPS_TYPE);
                put(OtherKey.MIDLLE_DEAL.REDIS_HEAD, StreamRedisConstants.HourKey.HOUR_GPS);

            }};

            GeneralDataStreamExecution<GpsHourSource, GpsHourTransfor,GpsHourLatestData, GpsHourCalcBiz> generalStreamExecution =
                    new GeneralDataStreamExecution<>()
                    .createJedis(jedis)
                    .createSpecialCalc(gpsHourCalcBiz)
                    .createSpecialSource(msg,StreamRedisConstants.HourKey.HOUR_GPS, DateTimeUtil.DEFAULT_DATE_HOUR)
                    ;


            IHBaseQueryAndPersistStrategy<GpsHourSource> iFirstStrategy=StrategyChoose.generateStrategy(prop.getProperty(PropKey.DEAL_STRATEGY),
                    httpUtils, prop.getProperty("url_base"), HttpURIUtil.GPS_HOUR_SOURCE_FIND,
                    connection, HBaseTable.HOUR_FIRST_ZONE.getTableName(),
                    HBaseTable.HOUR_FIRST_ZONE.getFirstFamilyName(), HbaseColumn.HourSourceColumn.gpsHourColumns,
                    GpsHourSource.class);

            IHBaseQueryAndPersistStrategy<GpsHourTransfor> iResultStrategy=StrategyChoose.generateStrategy(prop.getProperty(PropKey.DEAL_STRATEGY),
                    httpUtils, prop.getProperty("url_base"), HttpURIUtil.GPS_HOUR_FIND,
                    connection, HBaseTable.HOUR_STATISTICS.getTableName(),
                    HBaseTable.HOUR_STATISTICS.getSecondFamilyName(), HbaseColumn.HourStatisticsCloumn.gpsHourColumns,
                    GpsHourTransfor.class);

            generalStreamExecution.dealHourData(map,iFirstStrategy,iResultStrategy);

            List<String> persistValues = (List) map.get(OtherKey.MIDLLE_DEAL.PERSIST_KEY);
            if (CollectionUtils.isNotEmpty(persistValues)) {
                for (String str : persistValues) {
                    logger.debug("##persist gps hour data is {},list is {}", str, persistValues.toString());
                    collector.emit(StreamKey.GpsStream.GPS_HOUR_BOLT_S, new Values(str, generalStreamExecution.gentMsgId()));
                }
            }
            if(map.get(OtherKey.MIDLLE_DEAL.FIRST_TIME_ZONE)!=null)
                collector.emit(StreamKey.GpsStream.GPS_HOUR_BOLT_FIRST_DATA,
                    new Values(map.get(OtherKey.MIDLLE_DEAL.FIRST_TIME_ZONE),
                            generalStreamExecution.gentMsgId()));
        } catch (Throwable e) {
            logger.error("Process GPS hour data exception: {}", msg, e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(StreamKey.GpsStream.GPS_HOUR_BOLT_S, new Fields(new String[]{
                StreamKey.GpsStream.GPS_KEY_F, StreamKey.GpsStream.GPS_KEY_S}));
        outputFieldsDeclarer.declareStream(StreamKey.GpsStream.GPS_HOUR_BOLT_FIRST_DATA, new Fields(new String[]{
                StreamKey.GpsStream.GPS_KEY_F, StreamKey.GpsStream.GPS_KEY_S}));
    }

}

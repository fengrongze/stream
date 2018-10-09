package cst.jstorm.daymonth.bolt.gps;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.cst.stream.common.*;
import com.cst.stream.stathour.gps.GpsDayLatestData;
import com.cst.stream.stathour.gps.GpsDayTransfor;
import com.cst.stream.stathour.gps.GpsHourSource;
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
import cst.jstorm.daymonth.calcalations.gps.GpsDayCalcBiz;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import redis.clients.jedis.JedisCluster;

import java.util.*;

/**
 * @author Johnney.chiu
 * create on 2017/12/5 18:16
 * @Description gps 天数据计算
 */
public class GpsDayDataCalcBolt extends BaseBasicBolt{

    private static final int EXTIRE_TIME = 2 * 24 * 60 * 60;
    private static final long serialVersionUID = 8180968429882831272L;
    private transient Logger logger;
    private transient JedisCluster jedis;
    private AbstractApplicationContext beanContext;
    private Properties prop;
    private boolean forceLoad;
    private transient org.apache.hadoop.hbase.client.Connection connection;
    private transient HttpUtils httpUtils;
    private GpsDayCalcBiz gpsDayCalcBiz;


    public GpsDayDataCalcBolt(Properties prop, boolean forceLoad) {
        this.prop = prop;
        this.forceLoad = forceLoad;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf,context);
        prop = PropertiesUtil.initProp(prop, forceLoad);
        LogbackInitUtil.changeLogback(prop,true);
        if (beanContext == null) beanContext = MyApplicationContext.getDefaultContext();
        logger = LoggerFactory.getLogger(GpsDayDataCalcBolt.class);
        jedis = RedisUtil.buildJedisCluster(prop, RedisKey.STORM_REDISCLUSTER);
        connection = (org.apache.hadoop.hbase.client.Connection) beanContext.getBean(OtherKey.DataDealKey.HBASE_CONNECTION);
        httpUtils = (HttpUtils) beanContext.getBean(OtherKey.DataDealKey.HTTP_UTILS);
        gpsDayCalcBiz = new GpsDayCalcBiz();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void execute(Tuple input,BasicOutputCollector collector) {
        if (!StreamKey.GpsStream.GPS_HOUR_BOLT_F.equals(input.getSourceStreamId()))
            return;
        String msg = input.getString(0);
        if(StringUtils.isEmpty(msg))
            return;
        try {
            logger.debug("gps hour transfor data:{}",msg);
            Map map=new HashMap<String,Object>(){{
                put(OtherKey.DataDealKey.TIME_SELECT, CstConstants.TIME_SELECT.DAY);
                put(OtherKey.MIDLLE_DEAL.NEXT_KEY, new LinkedList<String>());
                put(OtherKey.MIDLLE_DEAL.FMT, DateTimeUtil.DEFAULT_DATE_DAY);
                put(OtherKey.MIDLLE_DEAL.INTERVAL, DateTimeUtil.ONE_DAY);
                put(OtherKey.MIDLLE_DEAL.LAST_DATA_PARAM, StreamTypeDefine.GPS_TYPE);
                put(OtherKey.MIDLLE_DEAL.BESINESS_KEY_TYPE,StreamTypeDefine.GPS_TYPE);
                put(OtherKey.MIDLLE_DEAL.REDIS_HEAD, StreamRedisConstants.DayKey.DAY_GPS);
                put(OtherKey.MIDLLE_DEAL.ZONE_VALUE_EXPIRE_TIME, prop.getProperty("day.zone.value.expire.time"));
                put(OtherKey.MIDLLE_DEAL.ZONE_SCHEDULE_EXPIRE_TIME, prop.getProperty("day.zone.schedule.expire.time"));

            }};

            GeneralDataStreamExecution<GpsHourSource, GpsDayTransfor,GpsDayLatestData, GpsDayCalcBiz> generalStreamExecution =
                    new GeneralDataStreamExecution<>().createJedis(jedis)
                    .createSpecialCalc(gpsDayCalcBiz)
                    .createSpecialSource(msg,StreamRedisConstants.DayKey.DAY_GPS, DateTimeUtil.DEFAULT_DATE_DAY)
                    ;

            IHBaseQueryAndPersistStrategy<GpsHourSource> iFirstStrategy=StrategyChoose.generateStrategy(prop.getProperty(PropKey.DEAL_STRATEGY),
                    httpUtils, prop.getProperty("url_base"), HttpURIUtil.GPS_DAY_SOURCE_FIND,
                    connection, HBaseTable.DAY_FIRST_ZONE.getTableName(),
                    HBaseTable.DAY_FIRST_ZONE.getFirstFamilyName(), HbaseColumn.DaySourceColumn.gpsDayColumns,
                    GpsHourSource.class);

            IHBaseQueryAndPersistStrategy<GpsDayTransfor> iResultStrategy=StrategyChoose.generateStrategy(prop.getProperty(PropKey.DEAL_STRATEGY),
                    httpUtils, prop.getProperty("url_base"), HttpURIUtil.GPS_DAY_FIND,
                    connection, HBaseTable.DAY_STATISTICS.getTableName(),
                    HBaseTable.DAY_STATISTICS.getSecondFamilyName(),
                    HbaseColumn.DayStatisticsCloumn.gpsDayColumns,
                    GpsDayTransfor.class);

            generalStreamExecution.dealDayData(map,iFirstStrategy,iResultStrategy);

            List<String> persistValues = (List) map.get(OtherKey.MIDLLE_DEAL.PERSIST_KEY);
            if (CollectionUtils.isNotEmpty(persistValues)) {
                for (String str : persistValues)
                    collector.emit(StreamKey.GpsStream.GPS_DAY_BOLT_S, new Values(str, generalStreamExecution.gentMsgId()));
            }
            if (map.get(OtherKey.MIDLLE_DEAL.FIRST_TIME_ZONE) != null) {
                collector.emit(StreamKey.GpsStream.GPS_DAY_BOLT_FIRST_DATA,
                        new Values(map.get(OtherKey.MIDLLE_DEAL.FIRST_TIME_ZONE),
                                generalStreamExecution.gentMsgId()));
            }
            List<String> nextValues = (List) map.get(OtherKey.MIDLLE_DEAL.NEXT_KEY);
            for(String str:nextValues) {
                logger.debug("##nextValue gps hour data is {},list is {}",str,nextValues.toString());
                collector.emit(StreamKey.GpsStream.GPS_MONTH_BOLT_F, new Values(str, generalStreamExecution.gentMsgId()));
                collector.emit(StreamKey.GpsStream.GPS_YEAR_BOLT_F, new Values(str, generalStreamExecution.gentMsgId()));
            }
        } catch (Throwable e) {
            logger.error("Process GPS day day exception: {}", msg, e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(StreamKey.GpsStream.GPS_DAY_BOLT_S, new Fields(new String[]{
                StreamKey.GpsStream.GPS_KEY_F, StreamKey.GpsStream.GPS_KEY_S}));
        declarer.declareStream(StreamKey.GpsStream.GPS_DAY_BOLT_FIRST_DATA, new Fields(new String[]{
                StreamKey.GpsStream.GPS_KEY_F, StreamKey.GpsStream.GPS_KEY_S}));
        declarer.declareStream(StreamKey.GpsStream.GPS_MONTH_BOLT_F, new Fields(new String[]{
                StreamKey.GpsStream.GPS_KEY_F, StreamKey.GpsStream.GPS_KEY_S}));
        declarer.declareStream(StreamKey.GpsStream.GPS_YEAR_BOLT_F, new Fields(new String[]{
                StreamKey.GpsStream.GPS_KEY_F, StreamKey.GpsStream.GPS_KEY_S}));
    }

    @Override
    public void cleanup() {
        super.cleanup();
        beanContext.close();
    }


}

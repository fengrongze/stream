package cst.jstorm.daymonth.bolt.tracedelete;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.cst.stream.common.*;
import com.cst.stream.stathour.tracedelete.TraceDeleteDayLatestData;
import com.cst.stream.stathour.tracedelete.TraceDeleteDayTransfor;
import com.cst.stream.stathour.tracedelete.TraceDeleteHourSource;
import com.fasterxml.jackson.core.JsonProcessingException;
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
import com.cst.jstorm.commons.utils.exceptions.NoSourceDataException;
import com.cst.jstorm.commons.utils.http.HttpUtils;
import com.cst.jstorm.commons.utils.spring.MyApplicationContext;
import cst.jstorm.daymonth.calcalations.tracedelete.TraceDeleteDayCalcBiz;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;
import java.text.ParseException;
import java.util.*;

/**
 * @author Johnney.Chiu
 * create on 2018/4/16 11:31
 * @Description 轨迹删除天计算
 * @title
 */
public class TraceDeleteDayDataCalcBolt extends BaseBasicBolt {
    private static final long serialVersionUID = 5955454266182630424L;
    private transient Logger logger;
    private AbstractApplicationContext beanContext;
    private transient JedisCluster jedis;
    private Properties prop;
    private boolean forceLoad;
    private transient org.apache.hadoop.hbase.client.Connection connection;
    private transient HttpUtils httpUtils;
    private TraceDeleteDayCalcBiz traceDeleteDayCalcBiz;

    public TraceDeleteDayDataCalcBolt(Properties prop, boolean forceLoad) {
        this.prop = prop;
        this.forceLoad = forceLoad;
    }

        @Override
        public void prepare(Map stormConf, TopologyContext context) {
            super.prepare(stormConf, context);
            prop = PropertiesUtil.initProp(prop, forceLoad);
            LogbackInitUtil.changeLogback(prop, true);
            logger = LoggerFactory.getLogger(TraceDeleteDayDataCalcBolt.class);
            jedis = RedisUtil.buildJedisCluster(prop, RedisKey.STORM_REDISCLUSTER);
            if (beanContext == null) beanContext = MyApplicationContext.getDefaultContext();
            connection = (org.apache.hadoop.hbase.client.Connection) beanContext.getBean(OtherKey.DataDealKey.HBASE_CONNECTION);
            httpUtils = (HttpUtils) beanContext.getBean(OtherKey.DataDealKey.HTTP_UTILS);
            traceDeleteDayCalcBiz = new TraceDeleteDayCalcBiz();
    }


        @Override
        @SuppressWarnings("unchecked")
        public void execute(Tuple tuple, BasicOutputCollector collector) {
        if (!StreamKey.TraceDeleteStream.TRACE_DELETE_HOUR_BOLT_F.equals(tuple.getSourceStreamId()))
            return;
        String msg = tuple.getString(0);
        if (StringUtils.isEmpty(msg))
            return;

        logger.debug("trace delete hour transfor data:{}", msg);

        try {
            Map map = new HashMap<String, Object>() {{
                put(OtherKey.DataDealKey.TIME_SELECT, CstConstants.TIME_SELECT.DAY);
                put(OtherKey.MIDLLE_DEAL.NEXT_KEY, new LinkedList<String>());
                put(OtherKey.MIDLLE_DEAL.FMT, DateTimeUtil.DEFAULT_DATE_DAY);
                put(OtherKey.MIDLLE_DEAL.INTERVAL, DateTimeUtil.ONE_DAY);
                put(OtherKey.MIDLLE_DEAL.BESINESS_KEY_TYPE,StreamTypeDefine.TRACE_DELETE_TYPE);
                put(OtherKey.MIDLLE_DEAL.LAST_DATA_PARAM, StreamTypeDefine.TRACE_DELETE_TYPE);
                put(OtherKey.MIDLLE_DEAL.ZONE_VALUE_EXPIRE_TIME, prop.getProperty("day.zone.value.expire.time"));
                put(OtherKey.MIDLLE_DEAL.REDIS_HEAD, StreamRedisConstants.DayKey.DAY_TRACE_DELETE);
                put(OtherKey.MIDLLE_DEAL.ZONE_SCHEDULE_EXPIRE_TIME, prop.getProperty("day.zone.schedule.expire.time"));


            }};
            GeneralDataStreamExecution<TraceDeleteHourSource, TraceDeleteDayTransfor,TraceDeleteDayLatestData, TraceDeleteDayCalcBiz> generalStreamExecution =
                    new GeneralDataStreamExecution<>()
                    .createJedis(jedis)
                    .createSpecialCalc(traceDeleteDayCalcBiz)
                    .createSpecialSource(msg, StreamRedisConstants.DayKey.DAY_TRACE_DELETE, DateTimeUtil.DEFAULT_DATE_DAY);
            //如果从缓存中拿到不是空数据

            IHBaseQueryAndPersistStrategy<TraceDeleteHourSource> iFirstStrategy = StrategyChoose.generateStrategy(prop.getProperty(PropKey.DEAL_STRATEGY),
                    httpUtils, prop.getProperty("url_base"), HttpURIUtil.TRACE_DELETE_DAY_SOURCE_FIND,
                    connection, HBaseTable.DAY_FIRST_ZONE.getTableName(),
                    HBaseTable.DAY_FIRST_ZONE.getFirstFamilyName(), HbaseColumn.DaySourceColumn.traceDeleteDayColumns,
                    TraceDeleteHourSource.class);

            IHBaseQueryAndPersistStrategy<TraceDeleteDayTransfor> iResultStrategy = StrategyChoose.generateStrategy(prop.getProperty(PropKey.DEAL_STRATEGY),
                    httpUtils, prop.getProperty("url_base"), HttpURIUtil.TRACE_DELETE_DAY_FIND,
                    connection, HBaseTable.DAY_STATISTICS.getTableName(),
                    HBaseTable.DAY_STATISTICS.getSixthFamilyName(), HbaseColumn.DayStatisticsCloumn.traceDeleteDayColumns,
                    TraceDeleteDayTransfor.class);
            generalStreamExecution.dealDayData(map,iFirstStrategy, iResultStrategy);

            List<String> persistValues = (List) map.get(OtherKey.MIDLLE_DEAL.PERSIST_KEY);
            if(CollectionUtils.isNotEmpty(persistValues))
                for (String str : persistValues)
                    collector.emit(StreamKey.TraceDeleteStream.TRACE_DELETE_DAY_BOLT_S, new Values(str, generalStreamExecution.gentMsgId()));

            if(map.get(OtherKey.MIDLLE_DEAL.FIRST_TIME_ZONE)!=null)
                collector.emit(StreamKey.TraceDeleteStream.TRACE_DELETE_DAY_BOLT_FIRST_DATA,
                        new Values(map.get(OtherKey.MIDLLE_DEAL.FIRST_TIME_ZONE),
                                generalStreamExecution.gentMsgId()));

            List<String> nextList = (List<String>) map.get(OtherKey.MIDLLE_DEAL.NEXT_KEY);
            for (String str : nextList) {
                logger.debug("##nextValue trace delete day data is {}", str);
                collector.emit(StreamKey.TraceDeleteStream.TRACE_DELETE_MONTH_BOLT_F, new Values(str, generalStreamExecution.gentMsgId()));
                collector.emit(StreamKey.TraceDeleteStream.TRACE_DELETE_YEAR_BOLT_F, new Values(str, generalStreamExecution.gentMsgId()));
            }
        } catch (NoSourceDataException e) {
            logger.error("execute trace delete day persist no source  data is{}:", msg, e);

        } catch (JsonProcessingException e) {
            logger.error("execute trace delete day persist data is{}:", msg, e, msg);
        } catch (ParseException e) {
            logger.error("execute trace delete day persist  data is{}:", msg, e, msg);
        } catch (IOException e) {
            logger.error("execute trace delete day persist   data is{}:", msg, e, msg);
        } catch (NullPointerException e) {
            logger.error("execute trace delete day persist  data is{}:", msg, e, msg);
        } catch (Exception e) {
            logger.error("execute trace delete day persist  data is{}:", msg, e, msg);
        }
    }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declareStream(StreamKey.TraceDeleteStream.TRACE_DELETE_DAY_BOLT_S, new Fields(new String[]{
                StreamKey.TraceDeleteStream.TRACE_DELETE_KEY_F, StreamKey.TraceDeleteStream.TRACE_DELETE_KEY_S}));

            declarer.declareStream(StreamKey.TraceDeleteStream.TRACE_DELETE_DAY_BOLT_FIRST_DATA, new Fields(new String[]{
                    StreamKey.TraceDeleteStream.TRACE_DELETE_KEY_F, StreamKey.TraceDeleteStream.TRACE_DELETE_KEY_S}));
            declarer.declareStream(StreamKey.TraceDeleteStream.TRACE_DELETE_MONTH_BOLT_F, new Fields(new String[]{
                    StreamKey.TraceDeleteStream.TRACE_DELETE_KEY_F, StreamKey.TraceDeleteStream.TRACE_DELETE_KEY_S}));
            declarer.declareStream(StreamKey.TraceDeleteStream.TRACE_DELETE_YEAR_BOLT_F, new Fields(new String[]{
                    StreamKey.TraceDeleteStream.TRACE_DELETE_KEY_F, StreamKey.TraceDeleteStream.TRACE_DELETE_KEY_S}));

        }

        @Override
        public void cleanup() {
        super.cleanup();
        beanContext.close();
    }
}

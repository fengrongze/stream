package cst.jstorm.hour.bolt.tracedelete;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.cst.stream.common.*;
import com.cst.stream.stathour.tracedelete.TraceDeleteHourLatestData;
import com.cst.stream.stathour.tracedelete.TraceDeleteHourSource;
import com.cst.stream.stathour.tracedelete.TraceDeleteHourTransfor;
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
import cst.jstorm.hour.calcalations.tracedelete.TraceDeleteHourCalcBiz;
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
 * create on 2018/4/13 17:01
 * @Description 轨迹删除小时计算
 * @title
 */
public class TraceDeleteHourDataCalcBolt extends BaseBasicBolt {


    private transient Logger logger;
    private transient JedisCluster jedis;
    private AbstractApplicationContext beanContext;
    private Properties prop;
    private boolean forceLoad;
    private transient org.apache.hadoop.hbase.client.Connection connection;
    private transient HttpUtils httpUtils;
    private TraceDeleteHourCalcBiz traceDeleteHourCalcBiz;

    public TraceDeleteHourDataCalcBolt(Properties prop, boolean forceLoad) {
        this.prop = prop;
        this.forceLoad = forceLoad;
    }
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf,context);
        prop = PropertiesUtil.initProp(prop, forceLoad);
        LogbackInitUtil.changeLogback(prop,true);
        jedis = RedisUtil.buildJedisCluster(prop, RedisKey.STORM_REDISCLUSTER);
        logger = LoggerFactory.getLogger(TraceDeleteHourDataCalcBolt.class);
        if (beanContext == null) beanContext = MyApplicationContext.getDefaultContext();
        connection = (org.apache.hadoop.hbase.client.Connection) beanContext.getBean(OtherKey.DataDealKey.HBASE_CONNECTION);
        httpUtils = (HttpUtils) beanContext.getBean(OtherKey.DataDealKey.HTTP_UTILS);
        traceDeleteHourCalcBiz = new TraceDeleteHourCalcBiz();
    }
    @Override
    @SuppressWarnings("unchecked")
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        if (!StreamKey.TraceDeleteStream.TRACE_DELETE_HOUR_BOLT_F.equals(tuple.getSourceStreamId()))
            return;
        String msg = tuple.getString(0);
        if(StringUtils.isEmpty(msg))
            return;
        try {

            logger.debug("trace delete hour source data:{}",msg);
            Map<String, Object> map = new HashMap<String,Object>(){{
                put(OtherKey.DataDealKey.TIME_SELECT, CstConstants.TIME_SELECT.HOUR);
                put(OtherKey.MIDLLE_DEAL.NEXT_KEY, new LinkedList<String>());
                //put(OtherKey.MIDLLE_DEAL.NEXT_STREAM, StreamKey.TraceDeleteStream.TRACE_DELETE_DAY_BOLT_F);
                put(OtherKey.MIDLLE_DEAL.FMT, DateTimeUtil.DEFAULT_DATE_HOUR);
                put(OtherKey.MIDLLE_DEAL.INTERVAL, DateTimeUtil.ONE_HOUR);
                put(OtherKey.MIDLLE_DEAL.LAST_DATA_PARAM, StreamTypeDefine.TRACE_DELETE_TYPE);
                put(OtherKey.MIDLLE_DEAL.LAST_VALUE_EXPIRE_TIME, prop.getProperty("last.value.expire.time"));
                put(OtherKey.MIDLLE_DEAL.ZONE_VALUE_EXPIRE_TIME, prop.getProperty("hour.zone.value.expire.time"));
                put(OtherKey.MIDLLE_DEAL.BESINESS_KEY_TYPE,StreamTypeDefine.TRACE_DELETE_TYPE);
                put(OtherKey.MIDLLE_DEAL.REDIS_HEAD, StreamRedisConstants.HourKey.HOUR_TRACE_DELETE);
            }};

            GeneralDataStreamExecution<TraceDeleteHourSource, TraceDeleteHourTransfor,TraceDeleteHourLatestData, TraceDeleteHourCalcBiz> generalStreamExecution =
                    new GeneralDataStreamExecution<>()
                    .createJedis(jedis)
                    .createSpecialCalc(traceDeleteHourCalcBiz)
                    .createSpecialSource(msg, StreamRedisConstants.HourKey.HOUR_TRACE_DELETE,
                            DateTimeUtil.DEFAULT_DATE_HOUR);

            IHBaseQueryAndPersistStrategy<TraceDeleteHourSource> iFirstStrategy = StrategyChoose.generateStrategy(prop.getProperty(PropKey.DEAL_STRATEGY),
                    httpUtils, prop.getProperty("url_base"), HttpURIUtil.TRACE_DELETE_HOUR_SOURCE_FIND,
                    connection, HBaseTable.HOUR_FIRST_ZONE.getTableName(),
                    HBaseTable.HOUR_FIRST_ZONE.getFirstFamilyName(), HbaseColumn.HourSourceColumn.traceDeleteHourColumns,
                    TraceDeleteHourSource.class);

            IHBaseQueryAndPersistStrategy<TraceDeleteHourTransfor> iResultStrategy = StrategyChoose.generateStrategy(prop.getProperty(PropKey.DEAL_STRATEGY),
                    httpUtils, prop.getProperty("url_base"), HttpURIUtil.TRACE_DELETE_HOUR_FIND,
                    connection, HBaseTable.HOUR_STATISTICS.getTableName(),
                    HBaseTable.HOUR_STATISTICS.getSixthFamilyName(), HbaseColumn.HourStatisticsCloumn.traceDeleteHourColumns,
                    TraceDeleteHourTransfor.class);
            generalStreamExecution.dealHourData(map,iFirstStrategy, iResultStrategy);


            List<String> persistValues = (List) map.get(OtherKey.MIDLLE_DEAL.PERSIST_KEY);
            if (CollectionUtils.isNotEmpty(persistValues)) {
                for (String str : persistValues) {
                    logger.debug("##persist trace delete hour data is {},list is {}", str, persistValues.toString());
                    collector.emit(StreamKey.TraceDeleteStream.TRACE_DELETE_HOUR_BOLT_S, new Values(str, generalStreamExecution.gentMsgId()));
                }
            }
            if(map.get(OtherKey.MIDLLE_DEAL.FIRST_TIME_ZONE)!=null)
                collector.emit(StreamKey.TraceDeleteStream.TRACE_DELETE_HOUR_BOLT_FIRST_DATA,
                        new Values(map.get(OtherKey.MIDLLE_DEAL.FIRST_TIME_ZONE),
                                generalStreamExecution.gentMsgId()));
        } catch (NoSourceDataException e) {
            logger.error("execute trace delete  hour persist no source data is{}:",msg, e);
        } catch (JsonProcessingException e) {
            logger.error("execute trace delete  hour persist  data is{}:",msg, e);
        } catch (ParseException e) {
            logger.error("execute trace delete  hour persist   data is{}:",msg, e);
        } catch (IOException e) {
            logger.error("execute trace delete  hour persist data is{}:",msg, e);
        } catch (NullPointerException e) {
            logger.error("execute trace delete  hour persist  data is{}:",msg,e);
        }catch (Exception e){
            logger.error("execute trace delete  hour persist data is{}:",msg, e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(StreamKey.TraceDeleteStream.TRACE_DELETE_HOUR_BOLT_S, new Fields(new String[]{
                StreamKey.TraceDeleteStream.TRACE_DELETE_KEY_F, StreamKey.TraceDeleteStream.TRACE_DELETE_KEY_S}));
        outputFieldsDeclarer.declareStream(StreamKey.TraceDeleteStream.TRACE_DELETE_HOUR_BOLT_FIRST_DATA, new Fields(new String[]{
                StreamKey.TraceDeleteStream.TRACE_DELETE_KEY_F, StreamKey.TraceDeleteStream.TRACE_DELETE_KEY_S}));
    }




    @Override
    public void cleanup() {
        super.cleanup();
        if (beanContext != null)
            beanContext.close();
    }
}

package cst.jstorm.hour.bolt.de;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.cst.stream.common.*;
import com.cst.stream.stathour.de.DeHourLatestData;
import com.cst.stream.stathour.de.DeHourSource;
import com.cst.stream.stathour.de.DeHourTransfor;
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
import cst.jstorm.hour.calcalations.de.DeHourCalcBiz;
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
 * @author Johnney.chiu
 * create on 2017/11/24 18:24
 * @Description De hour data calc
 */
public class DeHourDataCalcBolt extends BaseBasicBolt{
    private static final long serialVersionUID = 8675449039639946012L;
    private transient Logger logger;
    private transient JedisCluster jedis;
    private AbstractApplicationContext beanContext;
    private Properties prop;
    private boolean forceLoad;
    private transient org.apache.hadoop.hbase.client.Connection connection;
    private transient HttpUtils httpUtils;
    private DeHourCalcBiz deHourCalcBiz;

    public DeHourDataCalcBolt(Properties prop, boolean forceLoad) {
        this.prop = prop;
        this.forceLoad = forceLoad;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void execute(Tuple tuple,BasicOutputCollector collector) {
        if (!StreamKey.DeStream.DE_HOUR_BOLT_F.equals(tuple.getSourceStreamId())) {
            return;
        }
        String msg = tuple.getString(0);
        if (StringUtils.isEmpty(msg)) {
            return;
        }
        try {

            logger.debug("de hour source data:{}",msg);
            Map<String, Object> map = new HashMap<String,Object>(){{
                put(OtherKey.DataDealKey.TIME_SELECT, CstConstants.TIME_SELECT.HOUR);
                put(OtherKey.MIDLLE_DEAL.NEXT_KEY, new LinkedList<String>());
                //put(OtherKey.MIDLLE_DEAL.NEXT_STREAM, StreamKey.DeStream.DE_DAY_BOLT_F);
                put(OtherKey.MIDLLE_DEAL.FMT, DateTimeUtil.DEFAULT_DATE_HOUR);
                put(OtherKey.MIDLLE_DEAL.LAST_DATA_PARAM, StreamTypeDefine.DE_TYPE);
                put(OtherKey.MIDLLE_DEAL.INTERVAL, DateTimeUtil.ONE_HOUR);
                put(OtherKey.MIDLLE_DEAL.LAST_VALUE_EXPIRE_TIME, prop.getProperty("last.value.expire.time"));
                put(OtherKey.MIDLLE_DEAL.ZONE_VALUE_EXPIRE_TIME, prop.getProperty("hour.zone.value.expire.time"));
                put(OtherKey.MIDLLE_DEAL.BESINESS_KEY_TYPE,StreamTypeDefine.DE_TYPE);
                put(OtherKey.MIDLLE_DEAL.REDIS_HEAD, StreamRedisConstants.HourKey.HOUR_DE);
            }};

            GeneralDataStreamExecution<DeHourSource, DeHourTransfor,DeHourLatestData, DeHourCalcBiz> generalStreamExecution = new GeneralDataStreamExecution<>()
                    .createJedis(jedis)
                    .createSpecialCalc(deHourCalcBiz)
                    .createSpecialSource(msg, StreamRedisConstants.HourKey.HOUR_DE, DateTimeUtil.DEFAULT_DATE_HOUR);
            IHBaseQueryAndPersistStrategy<DeHourSource> iFirstZoneStrategy = StrategyChoose.generateStrategy(prop.getProperty(PropKey.DEAL_STRATEGY),
                    httpUtils, prop.getProperty("url_base"), HttpURIUtil.DE_HOUR_SOURCE_FIND,
                    connection, HBaseTable.HOUR_FIRST_ZONE.getTableName(),
                    HBaseTable.HOUR_FIRST_ZONE.getFirstFamilyName(), HbaseColumn.HourSourceColumn.deHourColumns,
                    DeHourSource.class);
            IHBaseQueryAndPersistStrategy<DeHourTransfor> iResultStrategy = StrategyChoose.generateStrategy(prop.getProperty(PropKey.DEAL_STRATEGY),
                    httpUtils, prop.getProperty("url_base"), HttpURIUtil.DE_HOUR_FIND,
                    connection, HBaseTable.HOUR_STATISTICS.getTableName(),
                    HBaseTable.HOUR_STATISTICS.getFourthFamilyName(), HbaseColumn.HourStatisticsCloumn.deHourColumns,
                    DeHourTransfor.class);
            generalStreamExecution.dealHourData(map,iFirstZoneStrategy,iResultStrategy);

            List<String> persistValues = (List) map.get(OtherKey.MIDLLE_DEAL.PERSIST_KEY);
            if (CollectionUtils.isNotEmpty(persistValues)) {
                for (String str : persistValues) {
                    logger.debug("##persist de hour data is {},list is {}", str, persistValues.toString());
                    collector.emit(StreamKey.DeStream.DE_HOUR_BOLT_S, new Values(str, generalStreamExecution.gentMsgId()));
                }
            }
            if (map.get(OtherKey.MIDLLE_DEAL.FIRST_TIME_ZONE) != null) {
                collector.emit(StreamKey.DeStream.DE_HOUR_BOLT_FIRST_DATA,
                        new Values(map.get(OtherKey.MIDLLE_DEAL.FIRST_TIME_ZONE),
                                generalStreamExecution.gentMsgId()));
            }
        } catch (NoSourceDataException e) {
            logger.error("execute de hour persist no source data data is{}:",msg, e);
        } catch (JsonProcessingException e) {
            logger.error("execute de hour persist data is{}:",msg, e);
        } catch (ParseException e) {
            logger.error("execute de hour persist data is{}:",msg, e);
        } catch (IOException e) {
            logger.error("execute de hour persist data is{}:",msg, e);
        } catch (NullPointerException e) {
            logger.error("execute de hour persist data is{}:",msg, e);
        }catch (Exception e){
            logger.error("execute de hour persist data is{}:",msg, e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(StreamKey.DeStream.DE_HOUR_BOLT_S, new Fields(new String[]{
                StreamKey.DeStream.DE_KEY_F, StreamKey.DeStream.DE_KEY_S}));
        outputFieldsDeclarer.declareStream(StreamKey.DeStream.DE_DAY_BOLT_F, new Fields(new String[]{
                StreamKey.DeStream.DE_KEY_F, StreamKey.DeStream.DE_KEY_S}));
        outputFieldsDeclarer.declareStream(StreamKey.DeStream.DE_HOUR_BOLT_FIRST_DATA, new Fields(new String[]{
                StreamKey.DeStream.DE_KEY_F, StreamKey.DeStream.DE_KEY_S}));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf,context);
        prop = PropertiesUtil.initProp(prop, forceLoad);
        LogbackInitUtil.changeLogback(prop,true);
        jedis = RedisUtil.buildJedisCluster(prop, RedisKey.STORM_REDISCLUSTER);
        logger = LoggerFactory.getLogger(DeHourDataCalcBolt.class);
        beanContext = MyApplicationContext.getDefaultContext();
        connection = (org.apache.hadoop.hbase.client.Connection) beanContext.getBean(OtherKey.DataDealKey.HBASE_CONNECTION);
        httpUtils = (HttpUtils) beanContext.getBean(OtherKey.DataDealKey.HTTP_UTILS);
        deHourCalcBiz = new DeHourCalcBiz();
    }


    @Override
    public void cleanup() {
        super.cleanup();
        if (beanContext != null) {
            beanContext.close();
        }
    }

}

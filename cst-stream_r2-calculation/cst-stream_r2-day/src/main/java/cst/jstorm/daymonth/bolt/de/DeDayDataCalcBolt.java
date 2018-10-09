package cst.jstorm.daymonth.bolt.de;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.cst.stream.common.*;
import com.cst.stream.stathour.de.DeDayLatestData;
import com.cst.stream.stathour.de.DeDayTransfor;
import com.cst.stream.stathour.de.DeHourSource;
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
import cst.jstorm.daymonth.calcalations.de.DeDayCalcBiz;
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
 * create on 2017/12/5 17:16
 * @Description de 天数据计算
 */
public class DeDayDataCalcBolt extends BaseBasicBolt {

    private static final long serialVersionUID = -6289290762240260838L;
    private transient Logger logger;
    private AbstractApplicationContext beanContext;
    private transient JedisCluster jedis;
    private Properties prop;
    private boolean forceLoad;
    private transient org.apache.hadoop.hbase.client.Connection connection;
    private transient HttpUtils httpUtils;
    private DeDayCalcBiz deDayCalcBiz;

    public DeDayDataCalcBolt(Properties prop, boolean forceLoad) {
        this.prop = prop;
        this.forceLoad = forceLoad;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        prop = PropertiesUtil.initProp(prop, forceLoad);
        LogbackInitUtil.changeLogback(prop,true);
        logger = LoggerFactory.getLogger(DeDayDataCalcBolt.class);
        jedis = RedisUtil.buildJedisCluster(prop, RedisKey.STORM_REDISCLUSTER);
        if (beanContext == null) beanContext = MyApplicationContext.getDefaultContext();
        connection = (org.apache.hadoop.hbase.client.Connection) beanContext.getBean(OtherKey.DataDealKey.HBASE_CONNECTION);
        httpUtils = (HttpUtils) beanContext.getBean(OtherKey.DataDealKey.HTTP_UTILS);
        deDayCalcBiz = new DeDayCalcBiz();
    }


    @Override
    @SuppressWarnings("unchecked")
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        if (!StreamKey.DeStream.DE_HOUR_BOLT_F.equals(tuple.getSourceStreamId()))
            return;
        String msg = tuple.getString(0);
        if(StringUtils.isEmpty(msg))
            return;

        logger.debug("de hour transfor data:{}",msg);

        try {
            Map map=new HashMap<String,Object>(){{
                put(OtherKey.DataDealKey.TIME_SELECT, CstConstants.TIME_SELECT.DAY);
                put(OtherKey.MIDLLE_DEAL.NEXT_KEY, new LinkedList<String>());
                put(OtherKey.MIDLLE_DEAL.LAST_DATA_PARAM, StreamTypeDefine.DE_TYPE);
                put(OtherKey.MIDLLE_DEAL.FMT, DateTimeUtil.DEFAULT_DATE_DAY);
                put(OtherKey.MIDLLE_DEAL.INTERVAL, DateTimeUtil.ONE_DAY);
                put(OtherKey.MIDLLE_DEAL.BESINESS_KEY_TYPE,StreamTypeDefine.DE_TYPE);
                put(OtherKey.MIDLLE_DEAL.REDIS_HEAD, StreamRedisConstants.DayKey.DAY_DE);
                put(OtherKey.MIDLLE_DEAL.ZONE_VALUE_EXPIRE_TIME, prop.getProperty("day.zone.value.expire.time"));
                put(OtherKey.MIDLLE_DEAL.ZONE_SCHEDULE_EXPIRE_TIME, prop.getProperty("day.zone.schedule.expire.time"));


            }};
            GeneralDataStreamExecution<DeHourSource, DeDayTransfor,DeDayLatestData, DeDayCalcBiz> generalStreamExecution =
                    new GeneralDataStreamExecution<>()
                    .createJedis(jedis)
                    .createSpecialCalc(deDayCalcBiz)
                    .createSpecialSource(msg, StreamRedisConstants.DayKey.DAY_DE, DateTimeUtil.DEFAULT_DATE_DAY);
            //如果从缓存中拿到不是空数据

            IHBaseQueryAndPersistStrategy<DeHourSource> iFirstZoneStrategy = StrategyChoose.generateStrategy(prop.getProperty(PropKey.DEAL_STRATEGY),
                    httpUtils, prop.getProperty("url_base"), HttpURIUtil.DE_DAY_SOURCE_FIND,
                    connection, HBaseTable.DAY_FIRST_ZONE.getTableName(),
                    HBaseTable.DAY_FIRST_ZONE.getFirstFamilyName(), HbaseColumn.DaySourceColumn.deDayColumns,
                    DeHourSource.class);
            IHBaseQueryAndPersistStrategy<DeDayTransfor> iResultStrategy = StrategyChoose.generateStrategy(prop.getProperty(PropKey.DEAL_STRATEGY),
                    httpUtils, prop.getProperty("url_base"), HttpURIUtil.DE_DAY_FIND,
                    connection, HBaseTable.DAY_STATISTICS.getTableName(),
                    HBaseTable.DAY_STATISTICS.getFourthFamilyName(), HbaseColumn.DayStatisticsCloumn.deDayColumns,
                    DeDayTransfor.class);
            generalStreamExecution.dealDayData(map,iFirstZoneStrategy,iResultStrategy);




            List<String> persistValues = (List) map.get(OtherKey.MIDLLE_DEAL.PERSIST_KEY);
            if (CollectionUtils.isNotEmpty(persistValues)) {
                for (String str : persistValues)
                    collector.emit(StreamKey.DeStream.DE_DAY_BOLT_S, new Values(str, generalStreamExecution.gentMsgId()));
            }

            if (map.get(OtherKey.MIDLLE_DEAL.FIRST_TIME_ZONE) != null) {
                collector.emit(StreamKey.DeStream.DE_DAY_BOLT_FIRST_DATA,
                        new Values(map.get(OtherKey.MIDLLE_DEAL.FIRST_TIME_ZONE),
                                generalStreamExecution.gentMsgId()));
            }
            List<String> nextValues = (List) map.get(OtherKey.MIDLLE_DEAL.NEXT_KEY);
            for(String str:nextValues) {
                logger.debug("##nextValue de hour data is {},list is {}",str,nextValues.toString());
                collector.emit(StreamKey.DeStream.DE_MONTH_BOLT_F, new Values(str, generalStreamExecution.gentMsgId()));
                collector.emit(StreamKey.DeStream.DE_YEAR_BOLT_F, new Values(str, generalStreamExecution.gentMsgId()));
            }

        } catch (NoSourceDataException e) {
            logger.error("execute de day persist no source data is{}:",msg, e);

        } catch (JsonProcessingException e) {
            logger.error("execute de day persist  data is{}:",msg, e);
        } catch (ParseException e) {
            logger.error("execute de day persist  data is{}:",msg, e);
        } catch (IOException e) {
            logger.error("execute de day persist data is{}:",msg, e);
        }catch (NullPointerException e) {
            logger.error("execute de day persist data is{}:",msg, e);
        }catch (Exception e){
            logger.error("execute de day persist data is{}:",msg, e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(StreamKey.DeStream.DE_DAY_BOLT_S, new Fields(new String[]{
                StreamKey.DeStream.DE_KEY_F, StreamKey.DeStream.DE_KEY_S}));
        declarer.declareStream(StreamKey.DeStream.DE_DAY_BOLT_FIRST_DATA, new Fields(new String[]{
                StreamKey.DeStream.DE_KEY_F, StreamKey.DeStream.DE_KEY_S}));
        declarer.declareStream(StreamKey.DeStream.DE_MONTH_BOLT_F, new Fields(new String[]{
                StreamKey.DeStream.DE_KEY_F, StreamKey.DeStream.DE_KEY_S}));
        declarer.declareStream(StreamKey.DeStream.DE_YEAR_BOLT_F, new Fields(new String[]{
                StreamKey.DeStream.DE_KEY_F, StreamKey.DeStream.DE_KEY_S}));
    }

    @Override
    public void cleanup() {
        super.cleanup();
        beanContext.close();
    }


}

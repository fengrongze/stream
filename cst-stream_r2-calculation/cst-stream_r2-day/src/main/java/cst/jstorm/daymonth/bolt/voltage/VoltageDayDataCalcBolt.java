package cst.jstorm.daymonth.bolt.voltage;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.cst.stream.common.*;
import com.cst.stream.stathour.voltage.VoltageDayLatestData;
import com.cst.stream.stathour.voltage.VoltageDayTransfor;
import com.cst.stream.stathour.voltage.VoltageHourSource;
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
import cst.jstorm.daymonth.calcalations.voltage.VoltageDayCalcBiz;
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
 * @Description 电瓶电压天计算
 * @title
 */
public class VoltageDayDataCalcBolt extends BaseBasicBolt {
    private static final long serialVersionUID = 889748081075082708L;
    private transient Logger logger;
    private AbstractApplicationContext beanContext;
    private transient JedisCluster jedis;
    private Properties prop;
    private boolean forceLoad;
    private transient org.apache.hadoop.hbase.client.Connection connection;
    private transient HttpUtils httpUtils;
    private VoltageDayCalcBiz voltageDayCalcBiz;
    public VoltageDayDataCalcBolt(Properties prop, boolean forceLoad) {
        this.prop = prop;
        this.forceLoad = forceLoad;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        prop = PropertiesUtil.initProp(prop, forceLoad);
        LogbackInitUtil.changeLogback(prop, true);
        logger = LoggerFactory.getLogger(VoltageDayDataCalcBolt.class);
        jedis = RedisUtil.buildJedisCluster(prop, RedisKey.STORM_REDISCLUSTER);
        if (beanContext == null) beanContext = MyApplicationContext.getDefaultContext();
        connection = (org.apache.hadoop.hbase.client.Connection) beanContext.getBean(OtherKey.DataDealKey.HBASE_CONNECTION);
        httpUtils = (HttpUtils) beanContext.getBean(OtherKey.DataDealKey.HTTP_UTILS);
        voltageDayCalcBiz = new VoltageDayCalcBiz();
    }


    @Override
    @SuppressWarnings("unchecked")
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        if (!StreamKey.VoltageStream.VOLTAGE_HOUR_BOLT_F.equals(tuple.getSourceStreamId()))
            return;
        String msg = tuple.getString(0);
        if (StringUtils.isEmpty(msg))
            return;

        logger.debug("voltage hour transfor data:{}", msg);

        try {
            Map map = new HashMap<String, Object>() {{
                put(OtherKey.DataDealKey.TIME_SELECT, CstConstants.TIME_SELECT.DAY);
                put(OtherKey.MIDLLE_DEAL.NEXT_KEY, new LinkedList<String>());
                put(OtherKey.MIDLLE_DEAL.FMT, DateTimeUtil.DEFAULT_DATE_DAY);
                put(OtherKey.MIDLLE_DEAL.INTERVAL, DateTimeUtil.ONE_DAY);
                put(OtherKey.MIDLLE_DEAL.LAST_DATA_PARAM, StreamTypeDefine.VOLTAGE_TYPE);
                put(OtherKey.MIDLLE_DEAL.BESINESS_KEY_TYPE,StreamTypeDefine.VOLTAGE_TYPE);
                put(OtherKey.MIDLLE_DEAL.ZONE_VALUE_EXPIRE_TIME, prop.getProperty("day.zone.value.expire.time"));
                put(OtherKey.MIDLLE_DEAL.REDIS_HEAD, StreamRedisConstants.DayKey.DAY_VOLTAGE);
                put(OtherKey.MIDLLE_DEAL.ZONE_SCHEDULE_EXPIRE_TIME, prop.getProperty("day.zone.schedule.expire.time"));


            }};
            GeneralDataStreamExecution<VoltageHourSource, VoltageDayTransfor,VoltageDayLatestData, VoltageDayCalcBiz> generalStreamExecution =
                    new GeneralDataStreamExecution<>()
                    .createJedis(jedis)
                    .createSpecialCalc(voltageDayCalcBiz)
                    .createSpecialSource(msg, StreamRedisConstants.DayKey.DAY_VOLTAGE, DateTimeUtil.DEFAULT_DATE_DAY);

            IHBaseQueryAndPersistStrategy<VoltageHourSource> iFirstStrategy = StrategyChoose.generateStrategy(prop.getProperty(PropKey.DEAL_STRATEGY),
                    httpUtils, prop.getProperty("url_base"), HttpURIUtil.VOLTAGE_DAY_SOURCE_FIND,
                    connection, HBaseTable.DAY_FIRST_ZONE.getTableName(),
                    HBaseTable.DAY_FIRST_ZONE.getFirstFamilyName(), HbaseColumn.DaySourceColumn.voltageDayColumns,
                    VoltageHourSource.class);

            IHBaseQueryAndPersistStrategy<VoltageDayTransfor> iResultStrategy =
                    StrategyChoose.generateStrategy(prop.getProperty(PropKey.DEAL_STRATEGY),
                    httpUtils, prop.getProperty("url_base"), HttpURIUtil.VOLTAGE_DAY_FIND,
                    connection, HBaseTable.DAY_STATISTICS.getTableName(),
                    HBaseTable.DAY_STATISTICS.getSeventhFamilyName(), HbaseColumn.DayStatisticsCloumn.voltageDayColumns,
                    VoltageDayTransfor.class);
            generalStreamExecution.dealDayData(map, iFirstStrategy,iResultStrategy);

            List<String> persistValues = (List) map.get(OtherKey.MIDLLE_DEAL.PERSIST_KEY);
            if(CollectionUtils.isNotEmpty(persistValues))
                for (String str : persistValues)
                    collector.emit(StreamKey.VoltageStream.VOLTAGE_DAY_BOLT_S, new Values(str, generalStreamExecution.gentMsgId()));

            if(map.get(OtherKey.MIDLLE_DEAL.FIRST_TIME_ZONE)!=null)
                collector.emit(StreamKey.VoltageStream.VOLTAGE_DAY_BOLT_FIRST_DATA,
                        new Values(map.get(OtherKey.MIDLLE_DEAL.FIRST_TIME_ZONE),
                                generalStreamExecution.gentMsgId()));

            List<String> nextList = (List<String>) map.get(OtherKey.MIDLLE_DEAL.NEXT_KEY);
            for (String str : nextList) {
                logger.debug("##nextValue voltage day data is {}", str);
                collector.emit(StreamKey.VoltageStream.VOLTAGE_MONTH_BOLT_F, new Values(str, generalStreamExecution.gentMsgId()));
                collector.emit(StreamKey.VoltageStream.VOLTAGE_YEAR_BOLT_F, new Values(str, generalStreamExecution.gentMsgId()));
            }
        } catch (NoSourceDataException e) {
            logger.error("execute voltage day persist data is{}:", msg, e);

        } catch (JsonProcessingException e) {
            logger.error("execute voltage day persist data is{}:", msg, e);
        } catch (ParseException e) {
            logger.error("execute voltage day persist   data is{}:", msg, e);
        } catch (IOException e) {
            logger.error("execute voltage day persist  data is{}:", msg, e);
        } catch (NullPointerException e) {
            logger.error("execute voltage day persist   data is{}:", msg, e);
        } catch (Exception e) {
            logger.error("execute voltage day persist   data is{}:", msg, e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(StreamKey.VoltageStream.VOLTAGE_DAY_BOLT_S, new Fields(new String[]{
                StreamKey.VoltageStream.VOLTAGE_KEY_F, StreamKey.VoltageStream.VOLTAGE_KEY_S}));
        declarer.declareStream(StreamKey.VoltageStream.VOLTAGE_DAY_BOLT_FIRST_DATA, new Fields(new String[]{
                StreamKey.VoltageStream.VOLTAGE_KEY_F, StreamKey.VoltageStream.VOLTAGE_KEY_S}));
        declarer.declareStream(StreamKey.VoltageStream.VOLTAGE_MONTH_BOLT_F, new Fields(new String[]{
                StreamKey.VoltageStream.VOLTAGE_KEY_F, StreamKey.VoltageStream.VOLTAGE_KEY_S}));
        declarer.declareStream(StreamKey.VoltageStream.VOLTAGE_YEAR_BOLT_F, new Fields(new String[]{
                StreamKey.VoltageStream.VOLTAGE_KEY_F, StreamKey.VoltageStream.VOLTAGE_KEY_S}));

    }

    @Override
    public void cleanup() {
        super.cleanup();
        beanContext.close();
    }
}
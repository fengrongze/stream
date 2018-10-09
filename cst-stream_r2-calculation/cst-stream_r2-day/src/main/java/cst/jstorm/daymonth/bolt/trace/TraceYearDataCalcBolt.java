package cst.jstorm.daymonth.bolt.trace;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.cst.stream.common.*;
import com.cst.stream.stathour.trace.TraceDayTransfor;
import com.cst.stream.stathour.trace.TraceYearTransfor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.cst.jstorm.commons.stream.constants.OtherKey;
import com.cst.jstorm.commons.stream.constants.PropKey;
import com.cst.jstorm.commons.stream.constants.RedisKey;
import com.cst.jstorm.commons.stream.constants.StreamKey;
import com.cst.jstorm.commons.stream.custom.CustomContextConfiguration;
import com.cst.jstorm.commons.stream.operations.GeneralAccumulationStreamExecution;
import com.cst.jstorm.commons.stream.operations.hbasestrategy.IHBaseQueryAndPersistStrategy;
import com.cst.jstorm.commons.stream.operations.hbasestrategy.StrategyChoose;
import com.cst.jstorm.commons.utils.HttpURIUtil;
import com.cst.jstorm.commons.utils.LogbackInitUtil;
import com.cst.jstorm.commons.utils.PropertiesUtil;
import com.cst.jstorm.commons.utils.RedisUtil;
import com.cst.jstorm.commons.utils.exceptions.NoSourceDataException;
import com.cst.jstorm.commons.utils.http.HttpUtils;
import com.cst.jstorm.commons.utils.spring.MyApplicationContext;
import cst.jstorm.daymonth.calcalations.trace.TraceYearCalcBiz;
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
 * create on 2017/12/18 16:16
 * @Description trace数据的年计算
 */
public class TraceYearDataCalcBolt extends BaseBasicBolt {

    private static final long serialVersionUID = -4316113077642283344L;
    private transient Logger logger;
    private transient JedisCluster jedis;
    private AbstractApplicationContext beanContext;
    private Properties prop;
    private boolean forceLoad;
    private transient org.apache.hadoop.hbase.client.Connection connection;
    private transient HttpUtils httpUtils;
    private TraceYearCalcBiz traceYearCalcBiz;

    public TraceYearDataCalcBolt(Properties prop, boolean forceLoad) {
        this.prop = prop;
        this.forceLoad = forceLoad;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        prop = PropertiesUtil.initProp(prop, forceLoad);
        LogbackInitUtil.changeLogback(prop, true);
        jedis = RedisUtil.buildJedisCluster(prop, RedisKey.STORM_REDISCLUSTER);
        beanContext = MyApplicationContext.getDefaultContext();
        logger = LoggerFactory.getLogger(TraceYearDataCalcBolt.class);

        connection = (org.apache.hadoop.hbase.client.Connection) beanContext.getBean(OtherKey.DataDealKey.HBASE_CONNECTION);
        httpUtils = (HttpUtils) beanContext.getBean(OtherKey.DataDealKey.HTTP_UTILS);
        traceYearCalcBiz = new TraceYearCalcBiz();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void execute(Tuple input, BasicOutputCollector collector) {
        if (!StreamKey.TraceStream.TRACE_YEAR_BOLT_F.equals(input.getSourceStreamId()))
            return;
        String msg = input.getString(0);
        if(StringUtils.isEmpty(msg))
            return;

        try {
            logger.debug("trace day transfor data:{}", msg);
            Map map = new HashMap<String, Object>() {{
                put(OtherKey.DataDealKey.TIME_SELECT, CstConstants.TIME_SELECT.YEAR);
                put(OtherKey.MIDLLE_DEAL.NEXT_KEY, new ArrayList<String>());
                put(OtherKey.MIDLLE_DEAL.TIME_FORMAT, DateTimeUtils.TimeFormat.LONG_DATE_PATTERN_LINE);
                put(OtherKey.MIDLLE_DEAL.ZONE_VALUE_EXPIRE_TIME, prop.getProperty("year.zone.value.expire.time"));
            }};
            GeneralAccumulationStreamExecution<TraceDayTransfor, TraceYearTransfor, TraceYearCalcBiz> generalStreamExecution = new GeneralAccumulationStreamExecution<>()
                    .createJedis(jedis)
                    .createSpecialCalc(traceYearCalcBiz)
                    .createSpecialSource(msg, StreamRedisConstants.YearKey.YEAR_TRACE, DateTimeUtil.DEFAULT_DATE_YEAR);
            //如果从缓存中拿到不是空数据
            IHBaseQueryAndPersistStrategy<TraceYearTransfor> iResultStrategy = StrategyChoose.generateStrategy(prop.getProperty(PropKey.DEAL_STRATEGY),
                    httpUtils, prop.getProperty("url_base"), HttpURIUtil.TRACE_YEAR_FIND,
                    connection, HBaseTable.YEAR_STATISTICS.getTableName(),
                    HBaseTable.YEAR_STATISTICS.getFifthFamilyName(),
                    HbaseColumn.YearStatisticsCloumn.traceYearColumns,
                    TraceYearTransfor.class);
            generalStreamExecution.dealAccumulationData(map, iResultStrategy);
            //hbase中拿不到该时区数据 查找最近一条上传的数据
            List<String> persistValues = (List) map.get(OtherKey.MIDLLE_DEAL.PERSIST_KEY);
            if(CollectionUtils.isNotEmpty(persistValues))
                for (String str : persistValues)
                    collector.emit(StreamKey.TraceStream.TRACE_YEAR_BOLT_S, new Values(str, generalStreamExecution.gentMsgId()));


        } catch (NoSourceDataException e) {
            logger.error("trace data execute,no source  data is{}:", msg, e);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            logger.error("trace data execute data is{}:", msg, e);
        } catch (ParseException e) {
            logger.error("trace data execute data is{}:", msg, e);
        } catch (IOException e) {
            logger.error("trace data execute data is{}:", msg, e);
        } catch (NullPointerException e) {
            logger.error("trace data execute  data is{}:", msg, e);
        } catch (Exception e) {
            logger.error("trace data execute  data is{}:", msg, e);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(StreamKey.TraceStream.TRACE_YEAR_BOLT_S, new Fields(new String[] {
                StreamKey.TraceStream.TRACE_KEY_F, StreamKey.TraceStream.TRACE_KEY_S}));
    }
    @Override
    public void cleanup() {
        super.cleanup();
        beanContext.close();
    }

}
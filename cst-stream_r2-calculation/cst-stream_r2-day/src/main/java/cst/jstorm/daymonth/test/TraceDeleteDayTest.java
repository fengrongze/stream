package cst.jstorm.daymonth.test;

import backtype.storm.Config;
import com.cst.config.client.ConfigClient;
import com.cst.jstorm.commons.stream.constants.OtherKey;
import com.cst.jstorm.commons.stream.constants.RedisKey;
import com.cst.jstorm.commons.stream.operations.GeneralDataStreamExecution;
import com.cst.jstorm.commons.stream.operations.hbasestrategy.IHBaseQueryAndPersistStrategy;
import com.cst.jstorm.commons.stream.operations.hbasestrategy.StrategyChoose;
import com.cst.jstorm.commons.utils.PropertiesUtil;
import com.cst.jstorm.commons.utils.RedisUtil;
import com.cst.jstorm.commons.utils.exceptions.NoSourceDataException;
import com.cst.jstorm.commons.utils.spring.MyApplicationContext;
import com.cst.stream.common.*;
import com.cst.stream.stathour.tracedelete.TraceDeleteDayLatestData;
import com.cst.stream.stathour.tracedelete.TraceDeleteDayTransfor;
import com.cst.stream.stathour.tracedelete.TraceDeleteHourSource;
import com.fasterxml.jackson.core.JsonProcessingException;
import cst.jstorm.daymonth.calcalations.tracedelete.TraceDeleteDayCalcBiz;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;
import java.text.ParseException;
import java.util.*;

public class TraceDeleteDayTest {
    private static Logger logger = LoggerFactory.getLogger(TraceDeleteDayTest.class);
    public static void main(String[] args) {
        TraceDeleteDayTest tddt = new TraceDeleteDayTest();
        TraceDeleteHourSource tdhs = new TraceDeleteHourSource();
        tdhs.setCarId("c6b04c9a254241c5b2cfa9531785419d");
        tdhs.setTime(1533364013000L);
        tdhs.setTraceId("25626737341833847");
        tdhs.setDinData("M201803310002_1533296542000");
        String msg = JsonHelper.toStringWithoutException(tdhs);
        logger.debug("trace delete hour transfor data:{}", msg);
        Properties prop = tddt.loadProp("config.properties");
        JedisCluster jedis = RedisUtil.buildJedisCluster(prop, RedisKey.STORM_REDISCLUSTER);
        AbstractApplicationContext beanContext = MyApplicationContext.getDefaultContext();
        org.apache.hadoop.hbase.client.Connection connection = (org.apache.hadoop.hbase.client.Connection) beanContext.getBean(OtherKey.DataDealKey.HBASE_CONNECTION);
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

            }};
            GeneralDataStreamExecution<TraceDeleteHourSource, TraceDeleteDayTransfor,TraceDeleteDayLatestData, TraceDeleteDayCalcBiz> generalStreamExecution =
                    new GeneralDataStreamExecution<>()
                            .createJedis(jedis)
                            .createSpecialCalc(new TraceDeleteDayCalcBiz())
                            .createSpecialSource(msg, StreamRedisConstants.DayKey.DAY_TRACE_DELETE, DateTimeUtil.DEFAULT_DATE_DAY);
            //如果从缓存中拿到不是空数据

            IHBaseQueryAndPersistStrategy<TraceDeleteHourSource> iFirstStrategy = StrategyChoose.generateStrategy(
                    connection, HBaseTable.DAY_FIRST_ZONE.getTableName(),
                    HBaseTable.DAY_FIRST_ZONE.getFirstFamilyName(), HbaseColumn.DaySourceColumn.traceDeleteDayColumns,
                    TraceDeleteHourSource.class);

            IHBaseQueryAndPersistStrategy<TraceDeleteDayTransfor> iResultStrategy = StrategyChoose.generateStrategy(
                    connection, HBaseTable.DAY_STATISTICS.getTableName(),
                    HBaseTable.DAY_STATISTICS.getSixthFamilyName(), HbaseColumn.DayStatisticsCloumn.traceDeleteDayColumns,
                    TraceDeleteDayTransfor.class);
            generalStreamExecution.dealDayData(map,iFirstStrategy, iResultStrategy);

            List<String> persistValues = (List) map.get(OtherKey.MIDLLE_DEAL.PERSIST_KEY);
           /* if(CollectionUtils.isNotEmpty(persistValues))
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
            }*/
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

    protected Properties loadProp(String localConfigName) {
        Properties prop = this.getLocalProperties(localConfigName);
        ConfigClient config = new ConfigClient();
        this.enrichConfigClient(config, prop);
        config.start();
        config.loadProperties(prop);
        config.stop();
        return prop;
    }

    public Properties getLocalProperties(String localConfigName) {
        Properties properties = new Properties();

        try {
            properties.load(PropertiesUtil.class.getClassLoader().getResourceAsStream(localConfigName));
        } catch (Exception var4) {
            logger.error("properties load failure", var4);
        }

        return properties;
    }

    public void enrichConfigClient(ConfigClient config, Properties properties) {
        config.setZookeeperUrl(properties.getProperty("zookeeper.url"));
        config.setLevels(properties.getProperty("levels"));
        config.setConfigFilename(properties.getProperty("config.filename"));
    }

    public Config initConf(Properties props, String param) {
        Config conf = new Config();
        return conf;
    }

    //protected abstract TopologyBuilder createBuilder(Properties var1, String var2);

    protected String getName(String pre, String param) {
        if (param.contains("obd")) {
            return pre + "_obd";
        } else if (param.contains("gps")) {
            return pre + "_gps";
        } else {
            return param.contains("integrated") ? pre + "_integrated" : pre;
        }
    }

}

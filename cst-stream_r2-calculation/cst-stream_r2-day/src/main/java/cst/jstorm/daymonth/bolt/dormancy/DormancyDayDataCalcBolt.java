package cst.jstorm.daymonth.bolt.dormancy;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.cst.jstorm.commons.stream.constants.OtherKey;
import com.cst.jstorm.commons.stream.constants.RedisKey;
import com.cst.jstorm.commons.stream.constants.StreamKey;
import com.cst.jstorm.commons.stream.operations.GeneralDataStreamExecution;
import com.cst.jstorm.commons.utils.LogbackInitUtil;
import com.cst.jstorm.commons.utils.PropertiesUtil;
import com.cst.jstorm.commons.utils.RedisUtil;
import com.cst.jstorm.commons.utils.spring.MyApplicationContext;
import com.cst.stream.common.*;
import com.cst.stream.stathour.am.AmDayLatestData;
import com.cst.stream.stathour.am.AmDayTransfor;
import com.cst.stream.stathour.am.AmHourSource;
import com.cst.stream.stathour.de.DeDayLatestData;
import com.cst.stream.stathour.de.DeDayTransfor;
import com.cst.stream.stathour.de.DeHourSource;
import com.cst.stream.stathour.dormancy.DormancySource;
import com.cst.stream.stathour.gps.GpsDayLatestData;
import com.cst.stream.stathour.gps.GpsDayTransfor;
import com.cst.stream.stathour.gps.GpsHourSource;
import com.cst.stream.stathour.mileage.MileageDayLatestData;
import com.cst.stream.stathour.mileage.MileageDayTransfor;
import com.cst.stream.stathour.mileage.MileageHourSource;
import com.cst.stream.stathour.obd.ObdDayLatestData;
import com.cst.stream.stathour.obd.ObdDayTransfor;
import com.cst.stream.stathour.obd.ObdHourSource;
import com.cst.stream.stathour.trace.TraceDayLatestData;
import com.cst.stream.stathour.trace.TraceDayTransfor;
import com.cst.stream.stathour.trace.TraceHourSource;
import com.cst.stream.stathour.tracedelete.TraceDeleteDayLatestData;
import com.cst.stream.stathour.tracedelete.TraceDeleteDayTransfor;
import com.cst.stream.stathour.tracedelete.TraceDeleteHourSource;
import com.cst.stream.stathour.voltage.VoltageDayLatestData;
import com.cst.stream.stathour.voltage.VoltageDayTransfor;
import com.cst.stream.stathour.voltage.VoltageHourSource;
import com.fasterxml.jackson.core.type.TypeReference;
import cst.jstorm.daymonth.calcalations.am.AmDayCalcBiz;
import cst.jstorm.daymonth.calcalations.de.DeDayCalcBiz;
import cst.jstorm.daymonth.calcalations.gps.GpsDayCalcBiz;
import cst.jstorm.daymonth.calcalations.mileage.MileageDayCalcBiz;
import cst.jstorm.daymonth.calcalations.obd.ObdDayCalcBiz;
import cst.jstorm.daymonth.calcalations.trace.TraceDayCalcBiz;
import cst.jstorm.daymonth.calcalations.tracedelete.TraceDeleteDayCalcBiz;
import cst.jstorm.daymonth.calcalations.voltage.VoltageDayCalcBiz;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import redis.clients.jedis.JedisCluster;

import java.util.*;

/**
 * @author Johnney.Chiu
 * create on 2018/8/10 14:57
 * @Description 休眠包
 * @title
 */
public class DormancyDayDataCalcBolt extends BaseBasicBolt {

    private transient Logger logger;
    private AbstractApplicationContext beanContext;
    private transient JedisCluster jedis;
    private Properties prop;
    private boolean forceLoad;
    private GpsDayCalcBiz gpsDayCalcBiz;
    private DeDayCalcBiz deDayCalcBiz;
    private AmDayCalcBiz amDayCalcBiz;
    private ObdDayCalcBiz obdDayCalcBiz;
    private TraceDayCalcBiz traceDayCalcBiz;
    private TraceDeleteDayCalcBiz traceDeleteDayCalcBiz;
    private VoltageDayCalcBiz voltageDayCalcBiz;
    private MileageDayCalcBiz mileageDayCalcBiz;


    public DormancyDayDataCalcBolt(Properties prop, boolean forceLoad) {
        this.prop = prop;
        this.forceLoad = forceLoad;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        prop = PropertiesUtil.initProp(prop, forceLoad);
        LogbackInitUtil.changeLogback(prop, true);
        beanContext = MyApplicationContext.getDefaultContext();
        logger = LoggerFactory.getLogger(DormancyDayDataCalcBolt.class);
        jedis = RedisUtil.buildJedisCluster(prop, RedisKey.STORM_REDISCLUSTER);

        gpsDayCalcBiz = new GpsDayCalcBiz();
        deDayCalcBiz = new DeDayCalcBiz();
        amDayCalcBiz = new AmDayCalcBiz();
        obdDayCalcBiz = new ObdDayCalcBiz();
        traceDayCalcBiz = new TraceDayCalcBiz();
        traceDeleteDayCalcBiz = new TraceDeleteDayCalcBiz();
        voltageDayCalcBiz = new VoltageDayCalcBiz();
        mileageDayCalcBiz = new MileageDayCalcBiz();

    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String msg = input.getString(0);
        if(StringUtils.isEmpty(msg))
            return;

        DormancySource source = JsonHelper.toBeanWithoutException(msg, new TypeReference<DormancySource>() {
        });

        Map map=new HashMap<String,Object>(){{
        }};
        transfor(source);
        doDormancy(map, collector, source);

    }

    private void doDormancy(Map map, BasicOutputCollector collector,DormancySource source){
        dealGpsDormancy(map, collector, source);
        dealAmDormancy(map, collector, source);
        dealObdDormancy(map, collector, source);
        dealDeDormancy(map, collector, source);
        dealTraceDormancy(map, collector, source);
        dealTraceDeleteDormancy(map, collector, source);
        dealVoltageDormancy(map, collector, source);
        dealMileageDormancy(map, collector, source);
    }

    private void transfor(DormancySource source) {
        source.setTime(DateTimeUtils.getMinusTime(source.getTime(), CstConstants.TIME_SELECT.DAY));
    }

    private void dealGpsDormancy(Map map, BasicOutputCollector collector,DormancySource source) {
        map.put(OtherKey.MIDLLE_DEAL.REDIS_HEAD, StreamRedisConstants.DayKey.DAY_GPS);
        map.put(OtherKey.MIDLLE_DEAL.PERSIST_KEY, null);
        try {
        GeneralDataStreamExecution<GpsHourSource, GpsDayTransfor, GpsDayLatestData, GpsDayCalcBiz> gpsGeneralStreamExecution =
                new GeneralDataStreamExecution<>().createJedis(jedis)
                        .createSpecialCalc(gpsDayCalcBiz)
                        ;
            gpsGeneralStreamExecution.createRedisKey(StreamRedisConstants.DayKey.DAY_GPS, DateTimeUtil.DEFAULT_DATE_DAY, source);
            gpsGeneralStreamExecution.executeDormancyData(map,source);
            List<String> persistValues = (List) map.get(OtherKey.MIDLLE_DEAL.PERSIST_KEY);
            if (CollectionUtils.isNotEmpty(persistValues)) {
                for (String str : persistValues)
                    collector.emit(StreamKey.GpsStream.GPS_DORMANCY_DAY_BOLT_S, new Values(str, source.getCarId()));
            }
        } catch (Throwable e) {
            logger.error(" execute  Dormancy source data init gps data error is{}", source,e);
        }
    }

    private void dealAmDormancy(Map map, BasicOutputCollector collector,DormancySource source) {
        map.put(OtherKey.MIDLLE_DEAL.REDIS_HEAD, StreamRedisConstants.DayKey.DAY_AM);
        map.put(OtherKey.MIDLLE_DEAL.PERSIST_KEY, null);
        try {
            GeneralDataStreamExecution<AmHourSource, AmDayTransfor, AmDayLatestData, AmDayCalcBiz> amGeneralStreamExecution =
                    new GeneralDataStreamExecution<>().createJedis(jedis)
                            .createSpecialCalc(amDayCalcBiz)
                    ;
            amGeneralStreamExecution.createRedisKey(StreamRedisConstants.DayKey.DAY_AM, DateTimeUtil.DEFAULT_DATE_DAY, source);
            amGeneralStreamExecution.executeDormancyData(map,source);
            List<String> persistValues = (List) map.get(OtherKey.MIDLLE_DEAL.PERSIST_KEY);
            if (CollectionUtils.isNotEmpty(persistValues)) {
                for (String str : persistValues)
                    collector.emit(StreamKey.AmStream.AM_DORMANCY_DAY_BOLT_S, new Values(str, source.getCarId()));
            }
        } catch (Throwable e) {
            logger.error(" execute  Dormancy source data init am data error is{}", source,e);
        }
    }

    private void dealObdDormancy(Map map, BasicOutputCollector collector,DormancySource source) {
        map.put(OtherKey.MIDLLE_DEAL.REDIS_HEAD, StreamRedisConstants.DayKey.DAY_OBD);
        map.put(OtherKey.MIDLLE_DEAL.PERSIST_KEY, null);
        try {
            GeneralDataStreamExecution<ObdHourSource, ObdDayTransfor, ObdDayLatestData, ObdDayCalcBiz> obdGeneralStreamExecution =
                    new GeneralDataStreamExecution<>().createJedis(jedis)
                            .createSpecialCalc(obdDayCalcBiz)
                    ;
            obdGeneralStreamExecution.createRedisKey(StreamRedisConstants.DayKey.DAY_OBD, DateTimeUtil.DEFAULT_DATE_DAY, source);
            obdGeneralStreamExecution.executeDormancyData(map,source);
            List<String> persistValues = (List) map.get(OtherKey.MIDLLE_DEAL.PERSIST_KEY);
            if (CollectionUtils.isNotEmpty(persistValues)) {
                for (String str : persistValues)
                    collector.emit(StreamKey.ObdStream.OBD_DORMANCY_DAY_BOLT_S, new Values(str, source.getCarId()));
            }
        } catch (Throwable e) {
            logger.error(" execute  Dormancy source data init obd data error is{}", source,e);
        }
    }
    private void dealDeDormancy(Map map, BasicOutputCollector collector,DormancySource source) {
        map.put(OtherKey.MIDLLE_DEAL.REDIS_HEAD, StreamRedisConstants.DayKey.DAY_DE);
        map.put(OtherKey.MIDLLE_DEAL.PERSIST_KEY, null);
        try {
            GeneralDataStreamExecution<DeHourSource, DeDayTransfor, DeDayLatestData, DeDayCalcBiz> deGeneralStreamExecution =
                    new GeneralDataStreamExecution<>().createJedis(jedis)
                            .createSpecialCalc(deDayCalcBiz)
                    ;
            deGeneralStreamExecution.createRedisKey(StreamRedisConstants.DayKey.DAY_DE, DateTimeUtil.DEFAULT_DATE_DAY, source);
            deGeneralStreamExecution.executeDormancyData(map,source);
            List<String> persistValues = (List) map.get(OtherKey.MIDLLE_DEAL.PERSIST_KEY);
            if (CollectionUtils.isNotEmpty(persistValues)) {
                for (String str : persistValues)
                    collector.emit(StreamKey.DeStream.DE_DORMANCY_DAY_BOLT_S, new Values(str, source.getCarId()));
            }
        } catch (Throwable e) {
            logger.error(" execute  Dormancy source data init de data error is{}", source,e);
        }
    }
    private void dealTraceDormancy(Map map, BasicOutputCollector collector,DormancySource source) {
        map.put(OtherKey.MIDLLE_DEAL.REDIS_HEAD, StreamRedisConstants.DayKey.DAY_TRACE);
        map.put(OtherKey.MIDLLE_DEAL.PERSIST_KEY, null);
        try {
            GeneralDataStreamExecution<TraceHourSource, TraceDayTransfor, TraceDayLatestData, TraceDayCalcBiz> traceGeneralStreamExecution =
                    new GeneralDataStreamExecution<>().createJedis(jedis)
                            .createSpecialCalc(traceDayCalcBiz)
                    ;
            traceGeneralStreamExecution.createRedisKey(StreamRedisConstants.DayKey.DAY_TRACE, DateTimeUtil.DEFAULT_DATE_DAY, source);
            traceGeneralStreamExecution.executeDormancyData(map,source);
            List<String> persistValues = (List) map.get(OtherKey.MIDLLE_DEAL.PERSIST_KEY);
            if (CollectionUtils.isNotEmpty(persistValues)) {
                for (String str : persistValues)
                    collector.emit(StreamKey.TraceStream.TRACE_DORMANCY_DAY_BOLT_S, new Values(str, source.getCarId()));
            }
        } catch (Throwable e) {
            logger.error(" execute  Dormancy source data init trace data error is{}", source,e);
        }
    }

    private void dealTraceDeleteDormancy(Map map, BasicOutputCollector collector,DormancySource source) {
        map.put(OtherKey.MIDLLE_DEAL.REDIS_HEAD, StreamRedisConstants.DayKey.DAY_TRACE_DELETE);
        map.put(OtherKey.MIDLLE_DEAL.PERSIST_KEY, null);
        try {
            GeneralDataStreamExecution<TraceDeleteHourSource, TraceDeleteDayTransfor, TraceDeleteDayLatestData, TraceDeleteDayCalcBiz> traceDeleteGeneralStreamExecution =
                    new GeneralDataStreamExecution<>().createJedis(jedis)
                            .createSpecialCalc(traceDeleteDayCalcBiz)
                    ;
            traceDeleteGeneralStreamExecution.createRedisKey(StreamRedisConstants.DayKey.DAY_TRACE_DELETE, DateTimeUtil.DEFAULT_DATE_DAY, source);
            traceDeleteGeneralStreamExecution.executeDormancyData(map,source);
            List<String> persistValues = (List) map.get(OtherKey.MIDLLE_DEAL.PERSIST_KEY);
            if (CollectionUtils.isNotEmpty(persistValues)) {
                for (String str : persistValues)
                    collector.emit(StreamKey.TraceDeleteStream.TRACE_DELETE_DORMANCY_DAY_BOLT_S, new Values(str, source.getCarId()));
            }
        } catch (Throwable e) {
            logger.error(" execute  Dormancy source data init trace delete data error is{}", source,e);
        }
    }

    private void dealVoltageDormancy(Map map, BasicOutputCollector collector,DormancySource source) {
        map.put(OtherKey.MIDLLE_DEAL.REDIS_HEAD, StreamRedisConstants.DayKey.DAY_VOLTAGE);
        map.put(OtherKey.MIDLLE_DEAL.PERSIST_KEY, null);
        try {
            GeneralDataStreamExecution<VoltageHourSource, VoltageDayTransfor, VoltageDayLatestData, VoltageDayCalcBiz> voltageGeneralStreamExecution =
                    new GeneralDataStreamExecution<>().createJedis(jedis)
                            .createSpecialCalc(voltageDayCalcBiz)
                    ;
            voltageGeneralStreamExecution.createRedisKey(StreamRedisConstants.DayKey.DAY_VOLTAGE, DateTimeUtil.DEFAULT_DATE_DAY, source);
            voltageGeneralStreamExecution.executeDormancyData(map,source);
            List<String> persistValues = (List) map.get(OtherKey.MIDLLE_DEAL.PERSIST_KEY);
            if (CollectionUtils.isNotEmpty(persistValues)) {
                for (String str : persistValues)
                    collector.emit(StreamKey.VoltageStream.VOLTAGE_DORMANCY_DAY_BOLT_S, new Values(str, source.getCarId()));
            }
        } catch (Throwable e) {
            logger.error(" execute  Dormancy source data init voltage data error is{}", source,e);
        }
    }
    private void dealMileageDormancy(Map map, BasicOutputCollector collector,DormancySource source) {
        map.put(OtherKey.MIDLLE_DEAL.REDIS_HEAD, StreamRedisConstants.DayKey.DAY_MILEAGE);
        map.put(OtherKey.MIDLLE_DEAL.PERSIST_KEY, null);
        try {
            GeneralDataStreamExecution<MileageHourSource, MileageDayTransfor, MileageDayLatestData, MileageDayCalcBiz> mileageGeneralStreamExecution =
                    new GeneralDataStreamExecution<>().createJedis(jedis)
                            .createSpecialCalc(mileageDayCalcBiz)
                    ;
            mileageGeneralStreamExecution.createRedisKey(StreamRedisConstants.DayKey.DAY_MILEAGE, DateTimeUtil.DEFAULT_DATE_DAY, source);
            mileageGeneralStreamExecution.executeDormancyData(map,source);
            List<String> persistValues = (List) map.get(OtherKey.MIDLLE_DEAL.PERSIST_KEY);
            if (CollectionUtils.isNotEmpty(persistValues)) {
                for (String str : persistValues)
                    collector.emit(StreamKey.MileageStream.MILEAGE_DORMANCY_DAY_BOLT_S, new Values(str, source.getCarId()));
            }
        } catch (Throwable e) {
            logger.error(" execute  Dormancy source data init mileage data error is{}", source,e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(StreamKey.GpsStream.GPS_DORMANCY_DAY_BOLT_S, new Fields(new String[]{
                StreamKey.GpsStream.GPS_KEY_F, StreamKey.GpsStream.GPS_KEY_S}));
        declarer.declareStream(StreamKey.AmStream.AM_DORMANCY_DAY_BOLT_S, new Fields(new String[]{
                StreamKey.AmStream.AM_KEY_F, StreamKey.AmStream.AM_KEY_S}));
        declarer.declareStream(StreamKey.ObdStream.OBD_DORMANCY_DAY_BOLT_S, new Fields(new String[]{
                StreamKey.ObdStream.OBD_KEY_F, StreamKey.ObdStream.OBD_KEY_S}));
        declarer.declareStream(StreamKey.DeStream.DE_DORMANCY_DAY_BOLT_S, new Fields(new String[]{
                StreamKey.DeStream.DE_KEY_F, StreamKey.DeStream.DE_KEY_S}));
        declarer.declareStream(StreamKey.VoltageStream.VOLTAGE_DORMANCY_DAY_BOLT_S, new Fields(new String[]{
                StreamKey.VoltageStream.VOLTAGE_KEY_F, StreamKey.VoltageStream.VOLTAGE_KEY_S}));
        declarer.declareStream(StreamKey.TraceStream.TRACE_DORMANCY_DAY_BOLT_S, new Fields(new String[]{
                StreamKey.TraceStream.TRACE_KEY_F, StreamKey.TraceStream.TRACE_KEY_S}));
        declarer.declareStream(StreamKey.TraceDeleteStream.TRACE_DELETE_DORMANCY_DAY_BOLT_S, new Fields(new String[]{
                StreamKey.TraceDeleteStream.TRACE_DELETE_KEY_F, StreamKey.TraceDeleteStream.TRACE_DELETE_KEY_S}));
        declarer.declareStream(StreamKey.MileageStream.MILEAGE_DORMANCY_DAY_BOLT_S, new Fields(new String[]{
                StreamKey.MileageStream.MILEAGE_KEY_F, StreamKey.MileageStream.MILEAGE_KEY_S}));
    }

    @Override
    public void cleanup() {
        super.cleanup();
        if (beanContext != null)
            beanContext.close();
    }
}

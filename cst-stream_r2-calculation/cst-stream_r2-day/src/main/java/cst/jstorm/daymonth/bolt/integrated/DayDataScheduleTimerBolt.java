package cst.jstorm.daymonth.bolt.integrated;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.cst.cmds.car.query.service.CarModelQueryService;
import com.cst.cmds.car.query.service.CarQueryService;
import com.cst.jstorm.commons.stream.constants.OtherKey;
import com.cst.jstorm.commons.stream.constants.RedisKey;
import com.cst.jstorm.commons.stream.constants.StreamKey;
import com.cst.jstorm.commons.stream.custom.CustomContextConfiguration;
import com.cst.jstorm.commons.stream.custom.GasProcess;
import com.cst.jstorm.commons.stream.custom.ProvinceProcess;
import com.cst.jstorm.commons.stream.operations.GeneralDataStreamExecution;
import com.cst.jstorm.commons.utils.HttpURIUtil;
import com.cst.jstorm.commons.utils.LogbackInitUtil;
import com.cst.jstorm.commons.utils.PropertiesUtil;
import com.cst.jstorm.commons.utils.RedisUtil;
import com.cst.jstorm.commons.utils.exceptions.NoSourceDataException;
import com.cst.jstorm.commons.utils.http.HttpUtils;
import com.cst.jstorm.commons.utils.spring.MyApplicationContext;
import com.cst.stream.common.CstConstants;
import com.cst.stream.common.DateTimeUtil;
import com.cst.stream.common.StreamRedisConstants;
import com.cst.stream.common.StreamTypeDefine;
import com.cst.stream.stathour.am.AmDayLatestData;
import com.cst.stream.stathour.am.AmDayTransfor;
import com.cst.stream.stathour.am.AmHourSource;
import com.cst.stream.stathour.de.DeDayLatestData;
import com.cst.stream.stathour.de.DeDayTransfor;
import com.cst.stream.stathour.de.DeHourSource;
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
import org.apache.commons.lang.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import redis.clients.jedis.JedisCluster;

import java.util.*;

/**
 * @author Johnney.Chiu
 * create on 2018/8/8 9:59
 * @Description 定时任务处理的bolt
 * @title
 */
public class DayDataScheduleTimerBolt extends BaseBasicBolt {

    private transient Logger logger;
    private transient JedisCluster jedis;
    private AbstractApplicationContext beanContext;
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

    private transient HttpUtils httpUtils;
    /** 车辆油单价缓存秒数 */
    private int CAR_GAS_PRICE_EXPIRE_SECONDS;


    public DayDataScheduleTimerBolt(Properties prop, boolean forceLoad) {
        this.prop = prop;
        this.forceLoad = forceLoad;

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        prop = PropertiesUtil.initProp(prop, forceLoad);
        LogbackInitUtil.changeLogback(prop, true);
        beanContext = MyApplicationContext.getDefaultContext();
        logger = LoggerFactory.getLogger(DayDataScheduleTimerBolt.class);
        jedis = RedisUtil.buildJedisCluster(prop, RedisKey.STORM_REDISCLUSTER);
        httpUtils = (HttpUtils) beanContext.getBean(OtherKey.DataDealKey.HTTP_UTILS);
        CAR_GAS_PRICE_EXPIRE_SECONDS = NumberUtils.toInt(prop.getProperty("gas.price.expire.time"), RedisKey.ExpireTime.GAS_PRICE_TIME);

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


        String carId =  input.getString(0);
        logger.info("get schedule carId:{}",carId);
        if (StringUtils.isBlank(carId))
            return;

        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE,-1);
        long dealTime = calendar.getTime().getTime();

        Map map = new HashMap<String, Object>() {{
            put(OtherKey.DataDealKey.TIME_SELECT, CstConstants.TIME_SELECT.DAY);
            put(OtherKey.MIDLLE_DEAL.FMT, DateTimeUtil.DEFAULT_DATE_DAY);

        }};
        logger.info("deal schedule carId:{}",carId);
        dealScheduleData(map,collector,carId,dealTime);

        logger.info("del schedule carId:{}",carId);
        //删除key
        carIdDelRedisCarSet(createCarKeySetToRedisKey(DateTimeUtil.DEFAULT_DATE_DAY, dealTime),carId);
        logger.info("del success schedule carId:{}",carId);
    }
    private void dealScheduleData(Map map, BasicOutputCollector collector,String carId,long time){
        dealGpsSchedule(map, collector, carId, time);
        dealAmSchedule(map, collector, carId, time);
        dealDeSchedule(map, collector, carId, time);
        dealObdSchedule(map, collector, carId, time);
        dealTraceSchedule(map, collector, carId, time);
        dealTraceDeleteSchedule(map, collector, carId, time);
        dealVoltageSchedule(map, collector, carId, time);
        dealMileageSchedule(map, collector, carId, time);

    }


    private void dealGpsSchedule(Map map, BasicOutputCollector collector,String carId,long time){
        logger.info("deal gps schedule carId:{}",carId);
        map.put(OtherKey.MIDLLE_DEAL.REDIS_HEAD, StreamRedisConstants.DayKey.DAY_GPS);
        map.put(OtherKey.MIDLLE_DEAL.PERSIST_KEY, null);
        //gps定时处理数据
        GeneralDataStreamExecution<GpsHourSource, GpsDayTransfor, GpsDayLatestData, GpsDayCalcBiz> gpsGeneralStreamExecution =
                new GeneralDataStreamExecution<>().createJedis(jedis)
                        .createSpecialCalc(gpsDayCalcBiz);
        try {
            gpsGeneralStreamExecution.createRedisKey(StreamRedisConstants.DayKey.DAY_GPS,DateTimeUtil.DEFAULT_DATE_DAY,carId,time);
            gpsGeneralStreamExecution.executeScheduleData(map);
            List<String> persistValues = (List) map.get(OtherKey.MIDLLE_DEAL.PERSIST_KEY);
            if (CollectionUtils.isNotEmpty(persistValues)) {
                for (String str : persistValues)
                    collector.emit(StreamKey.GpsStream.GPS_SCHEDULE_DAY_BOLT_S, new Values(str, carId));
            }
        } catch (Throwable e) {
            logger.error("try to schedule gps data carid {},time {} error ",carId,time,e);
        }
    }

    private void dealAmSchedule(Map map, BasicOutputCollector collector,String carId,long time){
        logger.info("deal am schedule carId:{}",carId);
        map.put(OtherKey.MIDLLE_DEAL.REDIS_HEAD, StreamRedisConstants.DayKey.DAY_AM);
        map.put(OtherKey.MIDLLE_DEAL.PERSIST_KEY, null);
        //am定时处理数据
        GeneralDataStreamExecution<AmHourSource, AmDayTransfor, AmDayLatestData, AmDayCalcBiz> amGeneralStreamExecution =
                new GeneralDataStreamExecution<>().createJedis(jedis)
                        .createSpecialCalc(amDayCalcBiz);
        try {
            amGeneralStreamExecution.createRedisKey(StreamRedisConstants.DayKey.DAY_AM,DateTimeUtil.DEFAULT_DATE_DAY,carId,time);
            amGeneralStreamExecution.executeScheduleData(map);
            List<String> persistValues = (List) map.get(OtherKey.MIDLLE_DEAL.PERSIST_KEY);
            if (CollectionUtils.isNotEmpty(persistValues)) {
                for (String str : persistValues)
                    collector.emit(StreamKey.AmStream.AM_SCHEDULE_DAY_BOLT_S, new Values(str, carId));
            }
        } catch (Throwable e) {
            logger.error("try to schedule am data carid {},time {} error ",carId,time,e);
        }
    }

    private void dealObdSchedule(Map map, BasicOutputCollector collector,String carId,long time){
        logger.info("deal obd schedule carId:{}",carId);
        map.put(OtherKey.MIDLLE_DEAL.REDIS_HEAD, StreamRedisConstants.DayKey.DAY_OBD);
        map.put(OtherKey.MIDLLE_DEAL.PERSIST_KEY, null);
        //obd定时处理数据
        GeneralDataStreamExecution<ObdHourSource, ObdDayTransfor, ObdDayLatestData, ObdDayCalcBiz> obdGeneralStreamExecution =
                new GeneralDataStreamExecution<>().createJedis(jedis)
                        .createSpecialCalc(obdDayCalcBiz);
        try {
            // 获取车辆的油单价
            String fuelPerPrice= "7.8" ;
            if (!Boolean.valueOf(prop.getProperty("ignore.outside.status"))) {
                fuelPerPrice = String.valueOf(GasProcess.getCarGasPrice(
                        prop.getProperty("url_base").concat(HttpURIUtil.CAR_PRICE_URL),
                        jedis, carId, time,
                        DateTimeUtil.toLongTimeString(time, DateTimeUtil.DEFAULT_DATE_DEFULT),
                        httpUtils, CAR_GAS_PRICE_EXPIRE_SECONDS
                ));

            }
            map.put("fuelPrice", StringUtils.isBlank(fuelPerPrice)?prop.getProperty("default_oil_price"):fuelPerPrice);
            obdGeneralStreamExecution.createRedisKey(StreamRedisConstants.DayKey.DAY_OBD,DateTimeUtil.DEFAULT_DATE_DAY,carId,time);
            obdGeneralStreamExecution.executeScheduleData(map);
            List<String> persistValues = (List) map.get(OtherKey.MIDLLE_DEAL.PERSIST_KEY);
            if (CollectionUtils.isNotEmpty(persistValues)) {
                for (String str : persistValues)
                    collector.emit(StreamKey.ObdStream.OBD_SCHEDULE_DAY_BOLT_S, new Values(str, carId));
            }
        } catch (Throwable e) {
            logger.error("try to schedule obd data carid {},time {} error ",carId,time,e);
        }
    }

    private void dealDeSchedule(Map map, BasicOutputCollector collector,String carId,long time){
        logger.info("deal de schedule carId:{}",carId);
        map.put(OtherKey.MIDLLE_DEAL.REDIS_HEAD, StreamRedisConstants.DayKey.DAY_DE);
        map.put(OtherKey.MIDLLE_DEAL.PERSIST_KEY, null);
        //de定时处理数据
        GeneralDataStreamExecution<DeHourSource, DeDayTransfor, DeDayLatestData, DeDayCalcBiz> deGeneralStreamExecution =
                new GeneralDataStreamExecution<>().createJedis(jedis)
                        .createSpecialCalc(deDayCalcBiz);
        try {
            deGeneralStreamExecution.createRedisKey(StreamRedisConstants.DayKey.DAY_DE,DateTimeUtil.DEFAULT_DATE_DAY,carId,time);
            deGeneralStreamExecution.executeScheduleData(map);
            List<String> persistValues = (List) map.get(OtherKey.MIDLLE_DEAL.PERSIST_KEY);
            if (CollectionUtils.isNotEmpty(persistValues)) {
                for (String str : persistValues)
                    collector.emit(StreamKey.DeStream.DE_SCHEDULE_DAY_BOLT_S, new Values(str, carId));
            }
        } catch (Throwable e) {
            logger.error("try to schedule de data carid {},time {} error ",carId,time,e);
        }
    }

    private void dealTraceSchedule(Map map, BasicOutputCollector collector,String carId,long time){
        logger.info("deal trace schedule carId:{}",carId);
        map.put(OtherKey.MIDLLE_DEAL.REDIS_HEAD, StreamRedisConstants.DayKey.DAY_TRACE);
        map.put(OtherKey.MIDLLE_DEAL.PERSIST_KEY, null);
        //de定时处理数据
        GeneralDataStreamExecution<TraceHourSource, TraceDayTransfor, TraceDayLatestData, TraceDayCalcBiz> traceGeneralStreamExecution =
                new GeneralDataStreamExecution<>().createJedis(jedis)
                        .createSpecialCalc(traceDayCalcBiz);
        try {
            traceGeneralStreamExecution.createRedisKey(StreamRedisConstants.DayKey.DAY_TRACE,DateTimeUtil.DEFAULT_DATE_DAY,carId,time);
            traceGeneralStreamExecution.executeScheduleData(map);
            List<String> persistValues = (List) map.get(OtherKey.MIDLLE_DEAL.PERSIST_KEY);
            if (CollectionUtils.isNotEmpty(persistValues)) {
                for (String str : persistValues)
                    collector.emit(StreamKey.TraceStream.TRACE_SCHEDULE_DAY_BOLT_S, new Values(str, carId));
            }
        } catch (Throwable e) {
            logger.error("try to schedule trace data carid {},time {} error ",carId,time,e);
        }
    }

    private void dealTraceDeleteSchedule(Map map, BasicOutputCollector collector,String carId,long time){
        logger.info("deal trace delete schedule carId:{}",carId);
        map.put(OtherKey.MIDLLE_DEAL.REDIS_HEAD, StreamRedisConstants.DayKey.DAY_TRACE_DELETE);
        map.put(OtherKey.MIDLLE_DEAL.PERSIST_KEY, null);
        //de定时处理数据
        GeneralDataStreamExecution<TraceDeleteHourSource, TraceDeleteDayTransfor, TraceDeleteDayLatestData, TraceDeleteDayCalcBiz> traceDeleteGeneralStreamExecution =
                new GeneralDataStreamExecution<>().createJedis(jedis)
                        .createSpecialCalc(traceDeleteDayCalcBiz);
        try {
            traceDeleteGeneralStreamExecution.createRedisKey(StreamRedisConstants.DayKey.DAY_TRACE_DELETE,DateTimeUtil.DEFAULT_DATE_DAY,carId,time);
            traceDeleteGeneralStreamExecution.executeScheduleData(map);
            List<String> persistValues = (List) map.get(OtherKey.MIDLLE_DEAL.PERSIST_KEY);
            if (CollectionUtils.isNotEmpty(persistValues)) {
                for (String str : persistValues)
                    collector.emit(StreamKey.TraceDeleteStream.TRACE_DELETE_SCHEDULE_DAY_BOLT_S, new Values(str, carId));
            }
        } catch (Throwable e) {
            logger.error("try to schedule trace delete data carid {},time {} error ",carId,time,e);
        }
    }

    private void dealVoltageSchedule(Map map, BasicOutputCollector collector,String carId,long time){
        logger.info("deal voltage schedule carId:{}",carId);
        map.put(OtherKey.MIDLLE_DEAL.REDIS_HEAD, StreamRedisConstants.DayKey.DAY_VOLTAGE);
        map.put(OtherKey.MIDLLE_DEAL.PERSIST_KEY, null);
        //voltage定时处理数据
        GeneralDataStreamExecution<VoltageHourSource, VoltageDayTransfor, VoltageDayLatestData, VoltageDayCalcBiz> voltageGeneralStreamExecution =
                new GeneralDataStreamExecution<>().createJedis(jedis)
                        .createSpecialCalc(voltageDayCalcBiz);
        try {
            voltageGeneralStreamExecution.createRedisKey(StreamRedisConstants.DayKey.DAY_VOLTAGE,DateTimeUtil.DEFAULT_DATE_DAY,carId,time);
            voltageGeneralStreamExecution.executeScheduleData(map);
            List<String> persistValues = (List) map.get(OtherKey.MIDLLE_DEAL.PERSIST_KEY);
            if (CollectionUtils.isNotEmpty(persistValues)) {
                for (String str : persistValues)
                    collector.emit(StreamKey.VoltageStream.VOLTAGE_SCHEDULE_DAY_BOLT_S, new Values(str, carId));
            }
        } catch (Throwable e) {
            logger.error("try to schedule voltage data carid {},time {} error ",carId,time,e);
        }
    }

    private void dealMileageSchedule(Map map, BasicOutputCollector collector,String carId,long time){
        logger.info("deal mileage schedule carId:{}",carId);
        map.put(OtherKey.MIDLLE_DEAL.REDIS_HEAD, StreamRedisConstants.DayKey.DAY_MILEAGE);
        map.put(OtherKey.MIDLLE_DEAL.PERSIST_KEY, null);
        //mileage定时处理数据
        GeneralDataStreamExecution<MileageHourSource, MileageDayTransfor, MileageDayLatestData, MileageDayCalcBiz> mileageGeneralStreamExecution =
                new GeneralDataStreamExecution<>().createJedis(jedis)
                        .createSpecialCalc(mileageDayCalcBiz);
        try {
            mileageGeneralStreamExecution.createRedisKey(StreamRedisConstants.DayKey.DAY_MILEAGE,DateTimeUtil.DEFAULT_DATE_DAY,carId,time);
            mileageGeneralStreamExecution.executeScheduleData(map);
            List<String> persistValues = (List) map.get(OtherKey.MIDLLE_DEAL.PERSIST_KEY);
            if (CollectionUtils.isNotEmpty(persistValues)) {
                for (String str : persistValues)
                    collector.emit(StreamKey.MileageStream.MILEAGE_SCHEDULE_DAY_BOLT_S, new Values(str, carId));
            }
        } catch (Throwable e) {
            logger.error("try to schedule mileage data carid {},time {} error ",carId,time,e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(StreamKey.GpsStream.GPS_SCHEDULE_DAY_BOLT_S, new Fields(new String[]{
                StreamKey.GpsStream.GPS_KEY_F, StreamKey.GpsStream.GPS_KEY_S}));
        declarer.declareStream(StreamKey.AmStream.AM_SCHEDULE_DAY_BOLT_S, new Fields(new String[]{
                StreamKey.AmStream.AM_KEY_F, StreamKey.AmStream.AM_KEY_S}));
        declarer.declareStream(StreamKey.ObdStream.OBD_SCHEDULE_DAY_BOLT_S, new Fields(new String[]{
                StreamKey.ObdStream.OBD_KEY_F, StreamKey.ObdStream.OBD_KEY_S}));
        declarer.declareStream(StreamKey.DeStream.DE_SCHEDULE_DAY_BOLT_S, new Fields(new String[]{
                StreamKey.DeStream.DE_KEY_F, StreamKey.DeStream.DE_KEY_S}));
        declarer.declareStream(StreamKey.VoltageStream.VOLTAGE_SCHEDULE_DAY_BOLT_S, new Fields(new String[]{
                StreamKey.VoltageStream.VOLTAGE_KEY_F, StreamKey.VoltageStream.VOLTAGE_KEY_S}));
        declarer.declareStream(StreamKey.TraceStream.TRACE_SCHEDULE_DAY_BOLT_S, new Fields(new String[]{
                StreamKey.TraceStream.TRACE_KEY_F, StreamKey.TraceStream.TRACE_KEY_S}));
        declarer.declareStream(StreamKey.TraceDeleteStream.TRACE_DELETE_SCHEDULE_DAY_BOLT_S, new Fields(new String[]{
                StreamKey.TraceDeleteStream.TRACE_DELETE_KEY_F, StreamKey.TraceDeleteStream.TRACE_DELETE_KEY_S}));
        declarer.declareStream(StreamKey.MileageStream.MILEAGE_SCHEDULE_DAY_BOLT_S, new Fields(new String[]{
                StreamKey.MileageStream.MILEAGE_KEY_F, StreamKey.MileageStream.MILEAGE_KEY_S}));

    }


    /**
     * 如果已经产生天结果,由Set删除
     * @param carKeySetKey
     * @param carId
     */
    private void carIdDelRedisCarSet(String carKeySetKey, String carId){
        jedis.srem(carKeySetKey,carId);
    }

    private  String createCarKeySetToRedisKey(String fmt,Long time) {
        return StreamRedisConstants.ZoneScheduleKey.getCarKeySetRedisKey(DateTimeUtil.toLongTimeString(time, fmt));
    }

    @Override
    public void cleanup() {
        super.cleanup();
        beanContext.close();
    }


}

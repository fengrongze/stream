package cst.jstorm.daymonth.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.cst.jstorm.commons.stream.constants.RedisKey;
import com.cst.jstorm.commons.stream.constants.StreamKey;
import com.cst.jstorm.commons.utils.LogbackInitUtil;
import com.cst.jstorm.commons.utils.PropertiesUtil;
import com.cst.jstorm.commons.utils.RedisUtil;
import com.cst.jstorm.commons.utils.spring.MyApplicationContext;
import com.cst.stream.common.DateTimeUtil;
import com.cst.stream.common.DateTimeUtils;
import com.cst.stream.common.StreamRedisConstants;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import redis.clients.jedis.JedisCluster;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * 定时加载缓存里面每天carid,计算天里程数据
 */
public class DayScheduleTimerSpout extends BaseRichSpout {
    private static final long serialVersionUID = 9058807200681257191L;
    private transient ScheduledExecutorService scheduler;
    private String timerTiming;
    private transient Logger logger;
    private AbstractApplicationContext beanContext;
    private transient JedisCluster jedis;
    private Properties prop;
    private boolean forceLoad;
    private SpoutOutputCollector collector;

    public DayScheduleTimerSpout(Properties prop, boolean forceLoad){
        this.prop = prop;
        this.forceLoad = forceLoad;
    }
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        prop = PropertiesUtil.initProp(prop, forceLoad);
        LogbackInitUtil.changeLogback(prop,true);
        beanContext = MyApplicationContext.getDefaultContext();
        logger = LoggerFactory.getLogger(DayScheduleTimerSpout.class);
        jedis = RedisUtil.buildJedisCluster(prop, RedisKey.STORM_REDISCLUSTER);
        timerTiming = prop.getProperty("day.mileage.timing", "01:00");
        /*this.scheduler = Executors.newScheduledThreadPool(1);

        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                handleData();
            }
        }, initialDelay(),24*60*60, SECONDS);*/
    }

    private void handleData(){
        try {
            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.DATE,-1);
            String key = StreamRedisConstants.ZoneScheduleKey.getCarKeySetRedisKey(DateTimeUtil.toLongTimeString(calendar.getTime(),DateTimeUtil.DEFAULT_DATE_DAY));
            Set<String> carIds = jedis.smembers(key);
            logger.info("-------------run begin--------------------------------");
            if(null!=carIds && carIds.size()>0){
                for (String carid:carIds){
                    logger.info("emit schedule carId:{}",carid);
                    collector.emit(StreamKey.ZoneScheduleStream.ZONE_SCHEDULED_DAY_BOLT_F,new Values(new String[]{carid}));
                }
            }

        } catch (Throwable e) {
            logger.warn("定时加载carid出错",e);
        }
    }
    @Override
    public void nextTuple() {
        clacScheduleData();
    }

    private void clacScheduleData(){
        String scheduleStart=prop.getProperty("schedule.start");
        String scheduleEnd = prop.getProperty("schedule.end");

        Date date = new Date();

        LocalDateTime localDateTimeStart= DateTimeUtils.getSpecialOfDateBymillSeconds(date, scheduleStart, DateTimeUtils.TimeFormat.SHORT_TIME_PATTERN_NONE_WITHOUT_MIS);
        LocalDateTime localDateTimeEnd=DateTimeUtils.getSpecialOfDateBymillSeconds(date, scheduleEnd, DateTimeUtils.TimeFormat.SHORT_TIME_PATTERN_NONE_WITHOUT_MIS);
        LocalDateTime now = LocalDateTime.now();
        try {
            //定时时间外的数据不启动
            if (now.isBefore(localDateTimeStart) || now.isAfter(localDateTimeEnd)) {
                Thread.sleep(60000);
                return;
            }
            String key = StreamRedisConstants.ZoneScheduleKey.getCarKeySetRedisKey(DateTimeUtil.toLongTimeString(
                    now.minusDays(1).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()
                    ,DateTimeUtil.DEFAULT_DATE_DAY));
            logger.info("run begin key :{}--------------------------------",key);
            Set<String> carIds = jedis.smembers(key);
            if(CollectionUtils.isNotEmpty(carIds)){
                logger.info("running size:{}--------------------------------",carIds.size());
                for (String carid:carIds){
                    logger.info("emit schedule carId:{}",carid);
                    collector.emit(StreamKey.ZoneScheduleStream.ZONE_SCHEDULED_DAY_BOLT_F,new Values(new String[]{carid}));
                }
                Thread.sleep(NumberUtils.toInt(prop.getProperty("schedule.one.per"),1800000));
            }else{
                Thread.sleep(NumberUtils.toInt(prop.getProperty("schedule.interval"),10000));
            }

        }catch (InterruptedException e) {
           logger.error("interruput error:{}");
        }


    }



    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(StreamKey.ZoneScheduleStream.ZONE_SCHEDULED_DAY_BOLT_F, new Fields(new String[]{StreamKey.ZoneScheduleStream.ZONE_SCHEDULED_KEY_F}));
    }

    /**
     * 初始化首次启动时间
     * @return
     */
    private long initialDelay(){
        Calendar confCal = getConfigCal();
        Calendar now = Calendar.getInstance();
        if(now.after(confCal)){
            confCal.add(Calendar.DATE,1);
        }

        Long initialDelay = (confCal.getTimeInMillis()-now.getTimeInMillis())/1000;
        logger.info("任务首次运行时间:{} ,相差:{} 秒", DateTimeUtil.toLongTimeString(confCal.getTime(),DateTimeUtil.DEFAULT_DATE_DEFULT),initialDelay);
        return initialDelay;
    }

    /**
     * 获取配置启动时间
     * @return
     */
    private Calendar getConfigCal(){
        timerTiming = StringUtils.isNotEmpty(timerTiming)?timerTiming:"01:00";
        String[] timerTimingArr = timerTiming.split(":");
        int hour=0;
        int minute=0;
        if(timerTimingArr.length==2){
            hour = Integer.parseInt(timerTimingArr[0]);
            minute = Integer.parseInt(timerTimingArr[1]);
        }else if(timerTimingArr.length==1){
            hour = Integer.parseInt(timerTimingArr[0]);
        }else {
            hour = 1;
        }

        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.HOUR_OF_DAY,hour);
        cal.set(Calendar.MINUTE,minute);

        return cal;
    }

    @Override
    public void deactivate() {
        super.deactivate();
        if(beanContext!=null)
            beanContext.close();
    }
}

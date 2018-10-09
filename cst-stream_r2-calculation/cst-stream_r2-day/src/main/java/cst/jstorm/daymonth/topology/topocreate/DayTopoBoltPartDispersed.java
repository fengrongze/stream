package cst.jstorm.daymonth.topology.topocreate;

import backtype.storm.topology.TopologyBuilder;
import com.cst.jstorm.commons.stream.constants.ComponentNameEnum;
import com.cst.jstorm.commons.stream.constants.StreamKey;
import com.cst.jstorm.commons.stream.custom.BoltCreateAdapter;
import cst.jstorm.daymonth.bolt.am.*;
import cst.jstorm.daymonth.bolt.de.*;
import cst.jstorm.daymonth.bolt.dormancy.DormancyDayDataCalcBolt;
import cst.jstorm.daymonth.bolt.gps.*;
import cst.jstorm.daymonth.bolt.integrated.DayDataScheduleTimerBolt;
import cst.jstorm.daymonth.bolt.dispatch.DispatchDayCalcBolt;
import cst.jstorm.daymonth.bolt.mileage.*;
import cst.jstorm.daymonth.bolt.obd.*;
import cst.jstorm.daymonth.bolt.trace.*;
import cst.jstorm.daymonth.bolt.tracedelete.*;
import cst.jstorm.daymonth.bolt.voltage.*;
import storm.trident.partition.IndexHashGrouping;

import java.util.Properties;

/**
 * @author Johnney.Chiu
 * create on 2018/5/14 15:23
 * @Description
 * @title
 */
public class DayTopoBoltPartDispersed extends BoltCreateAdapter {


    @Override
    public void buildScheduleTopo(Properties props, TopologyBuilder builder, int defaultTaskNum) {
        int scheduleDayDataCalcNum = Integer.valueOf(props.getProperty("storm.task.schedule.day.calc.num","1"));
        //天定时处理任务
        builder.setBolt(ComponentNameEnum.ZONE_SCHEDULED.getDayCalcBoltName(), new DayDataScheduleTimerBolt(props, false),
                scheduleDayDataCalcNum == 0 ? defaultTaskNum : scheduleDayDataCalcNum)
                .customGrouping(ComponentNameEnum.ZONE_SCHEDULED.getSpoutNameOther(),StreamKey.ZoneScheduleStream.ZONE_SCHEDULED_DAY_BOLT_F, new IndexHashGrouping(0));

    }

    @Override
    public void buildDormancyTopo(Properties props, TopologyBuilder builder, int defaultTaskNum) {
        int dormancyDayDataCalcNum = Integer.valueOf(props.getProperty("storm.task.day.dormancy.task.num","1"));
        //休眠包
        builder.setBolt(ComponentNameEnum.DORMANCY_SCHEDULED.getDayCalcBoltName(), new DormancyDayDataCalcBolt(props, false),
                dormancyDayDataCalcNum == 0 ? defaultTaskNum : dormancyDayDataCalcNum)
                .customGrouping(ComponentNameEnum.DORMANCY_SCHEDULED.getSpoutNameOther(),StreamKey.DormancyStream.DORMANCY_DAY_BOLT_F, new IndexHashGrouping(1));
    }

    @Override
    public void buildObdTopo(Properties props, TopologyBuilder builder, int defaultTaskNum) {

        int obdDayCalcNum = Integer.valueOf(props.getProperty("storm.task.obd.day.calc.num","1"));
        int obdDayPersistNum = Integer.valueOf(props.getProperty("storm.task.day.obd.persist.num","1"));
        int obdDayFirstPersistNum = Integer.valueOf(props.getProperty("storm.task.day.obd.first.persist.num","1"));

        int obdMonthCalcNum = Integer.valueOf(props.getProperty("storm.task.obd.month.calc.num","1"));
        int obdMonthPersistNum = Integer.valueOf(props.getProperty("storm.task.month.obd.persist.num","1"));
        int obdYearCalcNum = Integer.valueOf(props.getProperty("storm.task.obd.year.calc.num","1"));
        int obdYearPersistNum = Integer.valueOf(props.getProperty("storm.task.year.obd.persist.num","1"));

        //obd天数据计算
        builder.setBolt(ComponentNameEnum.OBD.getDayCalcBoltName(), new ObdDayDataCalcBolt(props,false),
                obdDayCalcNum==0?defaultTaskNum:obdDayCalcNum)
                .customGrouping(ComponentNameEnum.OBD.getSpoutNameOther(),StreamKey.ObdStream.OBD_HOUR_BOLT_F, new IndexHashGrouping(1))
                .customGrouping(ComponentNameEnum.ELECTRIC_OBD.getSpoutNameOther(),StreamKey.ElectricObdStream.ELECTRIC_OBD_HOUR_BOLT_F, new IndexHashGrouping(1));

        //obd天数据persist

        builder.setBolt(ComponentNameEnum.OBD.getDayPersistBoltName(), new ObdDayDataPersistBolt(props, false),
                obdDayPersistNum == 0 ? defaultTaskNum : obdDayPersistNum)
                .customGrouping(ComponentNameEnum.OBD.getDayCalcBoltName(), StreamKey.ObdStream.OBD_DAY_BOLT_S, new IndexHashGrouping(1))
                .customGrouping(ComponentNameEnum.ZONE_SCHEDULED.getDayCalcBoltName(), StreamKey.ObdStream.OBD_SCHEDULE_DAY_BOLT_S, new IndexHashGrouping(1))
                .customGrouping(ComponentNameEnum.DORMANCY_SCHEDULED.getDayCalcBoltName(), StreamKey.ObdStream.OBD_DORMANCY_DAY_BOLT_S, new IndexHashGrouping(1));


        builder.setBolt(ComponentNameEnum.OBD.getDayFirstSourceBoltName(), new ObdDayFirstDataPBolt(props, false),
                obdDayFirstPersistNum == 0 ? defaultTaskNum : obdDayFirstPersistNum)
                .customGrouping(ComponentNameEnum.OBD.getDayCalcBoltName(), StreamKey.ObdStream.OBD_DAY_BOLT_FIRST_DATA, new IndexHashGrouping(1));

        //month
        builder.setBolt(ComponentNameEnum.OBD.getMonthCalcBoltName(), new ObdMonthDataCalcBolt(props, false),
                obdMonthCalcNum == 0 ? defaultTaskNum : obdMonthCalcNum)
                .customGrouping(ComponentNameEnum.OBD.getDayCalcBoltName(), StreamKey.ObdStream.OBD_MONTH_BOLT_F, new IndexHashGrouping(1));
        builder.setBolt(ComponentNameEnum.OBD.getMonthPersistBoltName(), new ObdMonthDataPersistBolt(props, false),
                obdMonthPersistNum == 0 ? defaultTaskNum : obdMonthPersistNum)
                .customGrouping(ComponentNameEnum.OBD.getMonthCalcBoltName(), StreamKey.ObdStream.OBD_MONTH_BOLT_S, new IndexHashGrouping(1));
        //year
        builder.setBolt(ComponentNameEnum.OBD.getYearCalcBoltName(), new ObdYearDataCalcBolt(props, false),
                obdYearCalcNum == 0 ? defaultTaskNum : obdYearCalcNum)
                .customGrouping(ComponentNameEnum.OBD.getDayCalcBoltName(), StreamKey.ObdStream.OBD_YEAR_BOLT_F, new IndexHashGrouping(1));
        builder.setBolt(ComponentNameEnum.OBD.getYearPersistBoltName(), new ObdYearDataPersistBolt(props, false),
                obdYearPersistNum == 0 ? defaultTaskNum : obdYearPersistNum)
                .customGrouping(ComponentNameEnum.OBD.getYearCalcBoltName(), StreamKey.ObdStream.OBD_YEAR_BOLT_S, new IndexHashGrouping(1));


    }

    @Override
    public void buildGpsTopo(Properties props, TopologyBuilder builder, int defaultTaskNum) {

        int gpsDayCalcNum = Integer.valueOf(props.getProperty("storm.task.gps.day.calc.num","1"));
        int gpsDayPersistNum = Integer.valueOf(props.getProperty("storm.task.day.gps.persist.num","1"));
        int gpsDayFirstPersistNum = Integer.valueOf( props.getProperty("storm.task.day.gps.first.persist.num","1"));
        int gpsMonthCalcNum = Integer.valueOf(props.getProperty("storm.task.gps.month.calc.num","1"));
        int gpsMonthPersistNum = Integer.valueOf(props.getProperty("storm.task.month.gps.persist.num","1"));
        int gpsYearCalcNum = Integer.valueOf(props.getProperty("storm.task.gps.year.calc.num","1"));
        int gpsYearPersistNum = Integer.valueOf(props.getProperty("storm.task.year.gps.persist.num","1"));

        builder.setBolt(ComponentNameEnum.GPS.getDayCalcBoltName(), new GpsDayDataCalcBolt(props,false),
                gpsDayCalcNum==0?defaultTaskNum:gpsDayCalcNum)
                .customGrouping(ComponentNameEnum.GPS.getSpoutNameOther(),StreamKey.GpsStream.GPS_HOUR_BOLT_F, new IndexHashGrouping(1));

        builder.setBolt(ComponentNameEnum.GPS.getDayPersistBoltName(), new GpsDayDataPersistBolt(props, false),
                gpsDayPersistNum == 0 ? defaultTaskNum : gpsDayPersistNum)
                .customGrouping(ComponentNameEnum.GPS.getDayCalcBoltName(), StreamKey.GpsStream.GPS_DAY_BOLT_S, new IndexHashGrouping(1))
                .customGrouping(ComponentNameEnum.ZONE_SCHEDULED.getDayCalcBoltName(), StreamKey.GpsStream.GPS_SCHEDULE_DAY_BOLT_S, new IndexHashGrouping(1))
                .customGrouping(ComponentNameEnum.DORMANCY_SCHEDULED.getDayCalcBoltName(), StreamKey.GpsStream.GPS_DORMANCY_DAY_BOLT_S, new IndexHashGrouping(1));

        builder.setBolt(ComponentNameEnum.GPS.getDayFirstSourceBoltName(), new GpsDayFirstDataPBolt(props, false),
                gpsDayFirstPersistNum == 0 ? defaultTaskNum : gpsDayFirstPersistNum)
                .customGrouping(ComponentNameEnum.GPS.getDayCalcBoltName(), StreamKey.GpsStream.GPS_DAY_BOLT_FIRST_DATA, new IndexHashGrouping(1));

        //month
        builder.setBolt(ComponentNameEnum.GPS.getMonthCalcBoltName(), new GpsMonthDataCalcBolt(props, false),
                gpsMonthCalcNum == 0 ? defaultTaskNum : gpsMonthCalcNum)
                .customGrouping(ComponentNameEnum.GPS.getDayCalcBoltName(), StreamKey.GpsStream.GPS_MONTH_BOLT_F, new IndexHashGrouping(1));
        builder.setBolt(ComponentNameEnum.GPS.getMonthPersistBoltName(), new GpsMonthDataPersistBolt(props, false),
                gpsMonthPersistNum == 0 ? defaultTaskNum : gpsMonthPersistNum)
                .customGrouping(ComponentNameEnum.GPS.getMonthCalcBoltName(), StreamKey.GpsStream.GPS_MONTH_BOLT_S, new IndexHashGrouping(1));
        //year
        builder.setBolt(ComponentNameEnum.GPS.getYearCalcBoltName(), new GpsYearDataCalcBolt(props, false),
                gpsYearCalcNum == 0 ? defaultTaskNum : gpsYearCalcNum)
                .customGrouping(ComponentNameEnum.GPS.getDayCalcBoltName(), StreamKey.GpsStream.GPS_YEAR_BOLT_F, new IndexHashGrouping(1));
        builder.setBolt(ComponentNameEnum.GPS.getYearPersistBoltName(), new GpsYearDataPersistBolt(props, false),
                gpsYearPersistNum == 0 ? defaultTaskNum : gpsYearPersistNum)
                .customGrouping(ComponentNameEnum.GPS.getYearCalcBoltName(), StreamKey.GpsStream.GPS_YEAR_BOLT_S, new IndexHashGrouping(1));



    }

    @Override
    public void buildAmTopo(Properties props, TopologyBuilder builder, int defaultTaskNum) {

        int amDayCalcNum = Integer.valueOf( props.getProperty("storm.task.am.day.calc.num","1"));
        int amDayPersistNum = Integer.valueOf(props.getProperty("storm.task.day.am.persist.num","1"));
        int amDayFirstPersistNum = Integer.valueOf( props.getProperty("storm.task.day.am.first.persist.num","1"));
        int amMonthCalcNum = Integer.valueOf( props.getProperty("storm.task.am.month.calc.num","1"));
        int amMonthPersistNum = Integer.valueOf(props.getProperty("storm.task.month.am.persist.num","1"));
        int amYearCalcNum = Integer.valueOf( props.getProperty("storm.task.am.year.calc.num","1"));
        int amYearPersistNum = Integer.valueOf(props.getProperty("storm.task.year.am.persist.num","1"));


        builder.setBolt(ComponentNameEnum.AM.getDayCalcBoltName(), new AmDayDataCalcBolt(props,false),
                amDayCalcNum==0?defaultTaskNum:amDayCalcNum)
                .customGrouping(ComponentNameEnum.AM.getSpoutNameOther(), StreamKey.AmStream.AM_HOUR_BOLT_F, new IndexHashGrouping(1));

        builder.setBolt(ComponentNameEnum.AM.getDayPersistBoltName(), new AmDayDataPersistBolt(props, false),
                amDayPersistNum == 0 ? defaultTaskNum : amDayPersistNum)
                .customGrouping(ComponentNameEnum.AM.getDayCalcBoltName(), StreamKey.AmStream.AM_DAY_BOLT_S, new IndexHashGrouping(1))
                .customGrouping(ComponentNameEnum.ZONE_SCHEDULED.getDayCalcBoltName(), StreamKey.AmStream.AM_SCHEDULE_DAY_BOLT_S, new IndexHashGrouping(1))
                .customGrouping(ComponentNameEnum.DORMANCY_SCHEDULED.getDayCalcBoltName(), StreamKey.AmStream.AM_DORMANCY_DAY_BOLT_S, new IndexHashGrouping(1));

        builder.setBolt(ComponentNameEnum.AM.getDayFirstSourceBoltName(), new AmDayFirstDataPBolt(props, false),
                amDayFirstPersistNum == 0 ? defaultTaskNum : amDayFirstPersistNum)
                .customGrouping(ComponentNameEnum.AM.getDayCalcBoltName(), StreamKey.AmStream.AM_DAY_BOLT_FIRST_DATA, new IndexHashGrouping(1));


        //month
        builder.setBolt(ComponentNameEnum.AM.getMonthCalcBoltName(), new AmMonthDataCalcBolt(props, false),
                amMonthCalcNum == 0 ? defaultTaskNum : amMonthCalcNum)
                .customGrouping(ComponentNameEnum.AM.getDayCalcBoltName(), StreamKey.AmStream.AM_MONTH_BOLT_F, new IndexHashGrouping(1));
        builder.setBolt(ComponentNameEnum.AM.getMonthPersistBoltName(), new AmMonthDataPersistBolt(props, false),
                amMonthPersistNum == 0 ? defaultTaskNum : amMonthPersistNum)
                .customGrouping(ComponentNameEnum.AM.getMonthCalcBoltName(), StreamKey.AmStream.AM_MONTH_BOLT_S, new IndexHashGrouping(1));
        //year
        builder.setBolt(ComponentNameEnum.AM.getYearCalcBoltName(), new AmYearDataCalcBolt(props, false),
                amYearCalcNum == 0 ? defaultTaskNum : amYearCalcNum)
                .customGrouping(ComponentNameEnum.AM.getDayCalcBoltName(), StreamKey.AmStream.AM_YEAR_BOLT_F, new IndexHashGrouping(1));
        builder.setBolt(ComponentNameEnum.AM.getYearPersistBoltName(), new AmYearDataPersistBolt(props, false),
                amYearPersistNum == 0 ? defaultTaskNum : amYearPersistNum)
                .customGrouping(ComponentNameEnum.AM.getYearCalcBoltName(), StreamKey.AmStream.AM_YEAR_BOLT_S, new IndexHashGrouping(1));


    }

    @Override
    public void buildDeTopo(Properties props, TopologyBuilder builder, int defaultTaskNum) {

        int deDayCalcNum = Integer.valueOf( props.getProperty("storm.task.de.day.calc.num","1"));
        int deDayPersistNum = Integer.valueOf(props.getProperty("storm.task.day.de.persist.num","1"));
        int deDayFirstPersistNum = Integer.valueOf( props.getProperty("storm.task.day.de.first.persist.num","1"));
        int deMonthCalcNum = Integer.valueOf( props.getProperty("storm.task.de.month.calc.num","1"));
        int deMonthPersistNum = Integer.valueOf(props.getProperty("storm.task.month.de.persist.num","1"));
        int deYearCalcNum = Integer.valueOf( props.getProperty("storm.task.de.year.calc.num","1"));
        int deYearPersistNum = Integer.valueOf(props.getProperty("storm.task.year.de.persist.num","1"));
        builder.setBolt(ComponentNameEnum.DE.getDayCalcBoltName(), new DeDayDataCalcBolt(props,false),
                deDayCalcNum==0?defaultTaskNum:deDayCalcNum)
                .customGrouping(ComponentNameEnum.DE.getSpoutNameOther(),StreamKey.DeStream.DE_HOUR_BOLT_F, new IndexHashGrouping(1));
        builder.setBolt(ComponentNameEnum.DE.getDayPersistBoltName(), new DeDayDataPersistBolt(props, false),
                deDayPersistNum == 0 ? defaultTaskNum : deDayPersistNum)
                .customGrouping(ComponentNameEnum.DE.getDayCalcBoltName(), StreamKey.DeStream.DE_DAY_BOLT_S, new IndexHashGrouping(1))
                .customGrouping(ComponentNameEnum.ZONE_SCHEDULED.getDayCalcBoltName(), StreamKey.DeStream.DE_SCHEDULE_DAY_BOLT_S, new IndexHashGrouping(1))
                .customGrouping(ComponentNameEnum.DORMANCY_SCHEDULED.getDayCalcBoltName(), StreamKey.DeStream.DE_DORMANCY_DAY_BOLT_S, new IndexHashGrouping(1));

        builder.setBolt(ComponentNameEnum.DE.getDayFirstSourceBoltName(), new DeDayFirstDataPBolt(props, false),
                deDayFirstPersistNum == 0 ? defaultTaskNum : deDayFirstPersistNum)
                .customGrouping(ComponentNameEnum.DE.getDayCalcBoltName(), StreamKey.DeStream.DE_DAY_BOLT_FIRST_DATA, new IndexHashGrouping(1));

        //month
        builder.setBolt(ComponentNameEnum.DE.getMonthCalcBoltName(), new DeMonthDataCalcBolt(props, false),
                deMonthCalcNum == 0 ? defaultTaskNum : deMonthCalcNum)
                .customGrouping(ComponentNameEnum.DE.getDayCalcBoltName(), StreamKey.DeStream.DE_MONTH_BOLT_F, new IndexHashGrouping(1));
        builder.setBolt(ComponentNameEnum.DE.getMonthPersistBoltName(), new DeMonthDataPersistBolt(props, false),
                deMonthPersistNum == 0 ? defaultTaskNum : deMonthPersistNum)
                .customGrouping(ComponentNameEnum.DE.getMonthCalcBoltName(), StreamKey.DeStream.DE_MONTH_BOLT_S, new IndexHashGrouping(1));
        //year
        builder.setBolt(ComponentNameEnum.DE.getYearCalcBoltName(), new DeYearDataCalcBolt(props, false),
                deYearCalcNum == 0 ? defaultTaskNum : deYearCalcNum)
                .customGrouping(ComponentNameEnum.DE.getDayCalcBoltName(), StreamKey.DeStream.DE_YEAR_BOLT_F, new IndexHashGrouping(1));
        builder.setBolt(ComponentNameEnum.DE.getYearPersistBoltName(), new DeYearDataPersistBolt(props, false),
                deYearPersistNum == 0 ? defaultTaskNum : deYearPersistNum)
                .customGrouping(ComponentNameEnum.DE.getYearCalcBoltName(), StreamKey.DeStream.DE_YEAR_BOLT_S, new IndexHashGrouping(1));
    }

    @Override
    public void buildIntegratedTopo(Properties props, TopologyBuilder builder, int defaultTaskNum) {
        int integratedDayDispatchNum = Integer.valueOf(props.getProperty("storm.task.integrated.day.dispatch.num","1"));


        builder.setBolt(ComponentNameEnum.INTEGRATED.getDayDispatchBoltName(), new DispatchDayCalcBolt(props, false),
                integratedDayDispatchNum == 0 ? defaultTaskNum : integratedDayDispatchNum)
                .customGrouping(ComponentNameEnum.DE.getDayPersistBoltName(), StreamKey.ObdStream.DE_DAY_PERSIST_S, new IndexHashGrouping(1))
                .customGrouping(ComponentNameEnum.OBD.getDayPersistBoltName(), StreamKey.ObdStream.OBD_DAY_PERSIST_S, new IndexHashGrouping(1))
                .customGrouping(ComponentNameEnum.AM.getDayPersistBoltName(), StreamKey.ObdStream.AM_DAY_PERSIST_S, new IndexHashGrouping(1));

        }

    @Override
    public void buildTraceTopo(Properties props, TopologyBuilder builder, int defaultTaskNum) {

        int traceDayCalcNum = Integer.valueOf( props.getProperty("storm.task.trace.day.calc.num","1"));
        int traceDayPersistNum = Integer.valueOf(props.getProperty("storm.task.day.trace.persist.num","1"));
        int traceDayFirstPersistNum = Integer.valueOf( props.getProperty("storm.task.day.trace.first.persist.num","1"));
        int traceMonthCalcNum = Integer.valueOf( props.getProperty("storm.task.trace.month.calc.num","1"));
        int traceMonthPersistNum = Integer.valueOf(props.getProperty("storm.task.month.trace.persist.num","1"));
        int traceYearCalcNum = Integer.valueOf( props.getProperty("storm.task.trace.year.calc.num","1"));
        int traceYearPersistNum = Integer.valueOf(props.getProperty("storm.task.year.trace.persist.num","1"));



        builder.setBolt(ComponentNameEnum.TRACE.getDayCalcBoltName(), new TraceDayDataCalcBolt(props,false),
                traceDayCalcNum==0?defaultTaskNum:traceDayCalcNum)
                .customGrouping(ComponentNameEnum.TRACE.getSpoutNameOther(), StreamKey.TraceStream.TRACE_HOUR_BOLT_F, new IndexHashGrouping(1));

        builder.setBolt(ComponentNameEnum.TRACE.getDayPersistBoltName(), new TraceDayDataPersistBolt(props, false),
                traceDayPersistNum == 0 ? defaultTaskNum : traceDayPersistNum)
                .customGrouping(ComponentNameEnum.TRACE.getDayCalcBoltName(), StreamKey.TraceStream.TRACE_DAY_BOLT_S, new IndexHashGrouping(1))
                .customGrouping(ComponentNameEnum.ZONE_SCHEDULED.getDayCalcBoltName(), StreamKey.TraceStream.TRACE_SCHEDULE_DAY_BOLT_S, new IndexHashGrouping(1))
                .customGrouping(ComponentNameEnum.DORMANCY_SCHEDULED.getDayCalcBoltName(), StreamKey.TraceStream.TRACE_DORMANCY_DAY_BOLT_S, new IndexHashGrouping(1));

        builder.setBolt(ComponentNameEnum.TRACE.getDayFirstSourceBoltName(), new TraceDayFirstDataPBolt(props, false),
                traceDayFirstPersistNum == 0 ? defaultTaskNum : traceDayFirstPersistNum)
                .customGrouping(ComponentNameEnum.TRACE.getDayCalcBoltName(), StreamKey.TraceStream.TRACE_DAY_BOLT_FIRST_DATA, new IndexHashGrouping(1));


        //month
        builder.setBolt(ComponentNameEnum.TRACE.getMonthCalcBoltName(), new TraceMonthDataCalcBolt(props, false),
                traceMonthCalcNum == 0 ? defaultTaskNum : traceMonthCalcNum)
                .customGrouping(ComponentNameEnum.TRACE.getDayCalcBoltName(), StreamKey.TraceStream.TRACE_MONTH_BOLT_F, new IndexHashGrouping(1));
        builder.setBolt(ComponentNameEnum.TRACE.getMonthPersistBoltName(), new TraceMonthDataPersistBolt(props, false),
                traceMonthPersistNum == 0 ? defaultTaskNum : traceMonthPersistNum)
                .customGrouping(ComponentNameEnum.TRACE.getMonthCalcBoltName(), StreamKey.TraceStream.TRACE_MONTH_BOLT_S, new IndexHashGrouping(1));
        //year
        builder.setBolt(ComponentNameEnum.TRACE.getYearCalcBoltName(), new TraceYearDataCalcBolt(props, false),
                traceYearCalcNum == 0 ? defaultTaskNum : traceYearCalcNum)
                .customGrouping(ComponentNameEnum.TRACE.getDayCalcBoltName(), StreamKey.TraceStream.TRACE_YEAR_BOLT_F, new IndexHashGrouping(1));
        builder.setBolt(ComponentNameEnum.TRACE.getYearPersistBoltName(), new TraceYearDataPersistBolt(props, false),
                traceYearPersistNum == 0 ? defaultTaskNum : traceYearPersistNum)
                .customGrouping(ComponentNameEnum.TRACE.getYearCalcBoltName(), StreamKey.TraceStream.TRACE_YEAR_BOLT_S, new IndexHashGrouping(1));


    }

    @Override
    public void buildTraceDeleteTopo(Properties props, TopologyBuilder builder, int defaultTaskNum) {

        int traceDeleteDayCalcNum = Integer.valueOf( props.getProperty("storm.task.tracedelete.day.calc.num","1"));
        int traceDeleteDayPersistNum = Integer.valueOf(props.getProperty("storm.task.day.tracedelete.persist.num","1"));
        int traceDeleteDayFirstPersistNum = Integer.valueOf( props.getProperty("storm.task.day.tracedelete.first.persist.num","1"));
        int traceDeleteMonthCalcNum = Integer.valueOf( props.getProperty("storm.task.tracedelete.month.calc.num","1"));
        int traceDeleteMonthPersistNum = Integer.valueOf(props.getProperty("storm.task.month.tracedelete.persist.num","1"));
        int traceDeleteYearCalcNum = Integer.valueOf( props.getProperty("storm.task.tracedelete.year.calc.num","1"));
        int traceDeleteYearPersistNum = Integer.valueOf(props.getProperty("storm.task.year.tracedelete.persist.num","1"));




        builder.setBolt(ComponentNameEnum.TRACE_DELETE.getDayCalcBoltName(), new TraceDeleteDayDataCalcBolt(props,false),
                traceDeleteDayCalcNum==0?defaultTaskNum:traceDeleteDayCalcNum)
                .customGrouping(ComponentNameEnum.TRACE_DELETE.getSpoutNameOther(), StreamKey.TraceDeleteStream.TRACE_DELETE_HOUR_BOLT_F, new IndexHashGrouping(1));

        builder.setBolt(ComponentNameEnum.TRACE_DELETE.getDayPersistBoltName(), new TraceDeleteDayDataPersistBolt(props, false),
                traceDeleteDayPersistNum == 0 ? defaultTaskNum : traceDeleteDayPersistNum)
                .customGrouping(ComponentNameEnum.TRACE_DELETE.getDayCalcBoltName(), StreamKey.TraceDeleteStream.TRACE_DELETE_DAY_BOLT_S, new IndexHashGrouping(1))
                .customGrouping(ComponentNameEnum.ZONE_SCHEDULED.getDayCalcBoltName(), StreamKey.TraceDeleteStream.TRACE_DELETE_SCHEDULE_DAY_BOLT_S, new IndexHashGrouping(1))
                .customGrouping(ComponentNameEnum.DORMANCY_SCHEDULED.getDayCalcBoltName(), StreamKey.TraceDeleteStream.TRACE_DELETE_DORMANCY_DAY_BOLT_S, new IndexHashGrouping(1));

        builder.setBolt(ComponentNameEnum.TRACE_DELETE.getDayFirstSourceBoltName(), new TraceDeleteDayFirstDataPBolt(props, false),
                traceDeleteDayFirstPersistNum == 0 ? defaultTaskNum : traceDeleteDayFirstPersistNum)
                .customGrouping(ComponentNameEnum.TRACE_DELETE.getDayCalcBoltName(), StreamKey.TraceDeleteStream.TRACE_DELETE_DAY_BOLT_FIRST_DATA, new IndexHashGrouping(1));
        //month
        builder.setBolt(ComponentNameEnum.TRACE_DELETE.getMonthCalcBoltName(), new TraceDeleteMonthDataCalcBolt(props, false),
                traceDeleteMonthCalcNum == 0 ? defaultTaskNum : traceDeleteMonthCalcNum)
                .customGrouping(ComponentNameEnum.TRACE_DELETE.getDayCalcBoltName(), StreamKey.TraceDeleteStream.TRACE_DELETE_MONTH_BOLT_F, new IndexHashGrouping(1));
        builder.setBolt(ComponentNameEnum.TRACE_DELETE.getMonthPersistBoltName(), new TraceDeleteMonthDataPersistBolt(props, false),
                traceDeleteMonthPersistNum == 0 ? defaultTaskNum : traceDeleteMonthPersistNum)
                .customGrouping(ComponentNameEnum.TRACE_DELETE.getMonthCalcBoltName(), StreamKey.TraceDeleteStream.TRACE_DELETE_MONTH_BOLT_S, new IndexHashGrouping(1));
        //year
        builder.setBolt(ComponentNameEnum.TRACE_DELETE.getYearCalcBoltName(), new TraceDeleteYearDataCalcBolt(props, false),
                traceDeleteYearCalcNum == 0 ? defaultTaskNum : traceDeleteYearCalcNum)
                .customGrouping(ComponentNameEnum.TRACE_DELETE.getDayCalcBoltName(), StreamKey.TraceDeleteStream.TRACE_DELETE_YEAR_BOLT_F, new IndexHashGrouping(1));
        builder.setBolt(ComponentNameEnum.TRACE_DELETE.getYearPersistBoltName(), new TraceDeleteYearDataPersistBolt(props, false),
                traceDeleteYearPersistNum == 0 ? defaultTaskNum : traceDeleteYearPersistNum)
                .customGrouping(ComponentNameEnum.TRACE_DELETE.getYearCalcBoltName(), StreamKey.TraceDeleteStream.TRACE_DELETE_YEAR_BOLT_S, new IndexHashGrouping(1));


    }

    @Override
    public void buildVoltageTopo(Properties props, TopologyBuilder builder, int defaultTaskNum) {
        int voltageDayCalcNum = Integer.valueOf( props.getProperty("storm.task.voltage.day.calc.num","1"));
        int voltageDayPersistNum = Integer.valueOf(props.getProperty("storm.task.day.voltage.persist.num","1"));
        int voltageDayFirstPersistNum = Integer.valueOf( props.getProperty("storm.task.day.voltage.first.persist.num","1"));
        int voltageMonthCalcNum = Integer.valueOf( props.getProperty("storm.task.voltage.month.calc.num","1"));
        int voltageMonthPersistNum = Integer.valueOf(props.getProperty("storm.task.month.voltage.persist.num","1"));
        int voltageYearCalcNum = Integer.valueOf( props.getProperty("storm.task.voltage.year.calc.num","1"));
        int voltageYearPersistNum = Integer.valueOf(props.getProperty("storm.task.year.voltage.persist.num","1"));

        builder.setBolt(ComponentNameEnum.VOLTAGE.getDayCalcBoltName(), new VoltageDayDataCalcBolt(props,false),
                voltageDayCalcNum==0?defaultTaskNum:voltageDayCalcNum)
                .customGrouping(ComponentNameEnum.VOLTAGE.getSpoutNameOther(), StreamKey.VoltageStream.VOLTAGE_HOUR_BOLT_F, new IndexHashGrouping(1));

        builder.setBolt(ComponentNameEnum.VOLTAGE.getDayPersistBoltName(), new VoltageDayDataPersistBolt(props, false),
                voltageDayPersistNum == 0 ? defaultTaskNum : voltageDayPersistNum)
                .customGrouping(ComponentNameEnum.VOLTAGE.getDayCalcBoltName(), StreamKey.VoltageStream.VOLTAGE_DAY_BOLT_S, new IndexHashGrouping(1))
                .customGrouping(ComponentNameEnum.ZONE_SCHEDULED.getDayCalcBoltName(), StreamKey.VoltageStream.VOLTAGE_SCHEDULE_DAY_BOLT_S, new IndexHashGrouping(1))
                .customGrouping(ComponentNameEnum.DORMANCY_SCHEDULED.getDayCalcBoltName(), StreamKey.VoltageStream.VOLTAGE_DORMANCY_DAY_BOLT_S, new IndexHashGrouping(1));

        builder.setBolt(ComponentNameEnum.VOLTAGE.getDayFirstSourceBoltName(), new VoltageDayFirstDataPBolt(props, false),
                voltageDayFirstPersistNum == 0 ? defaultTaskNum : voltageDayFirstPersistNum)
                .customGrouping(ComponentNameEnum.VOLTAGE.getDayCalcBoltName(), StreamKey.VoltageStream.VOLTAGE_DAY_BOLT_FIRST_DATA, new IndexHashGrouping(1));

        //month
        builder.setBolt(ComponentNameEnum.VOLTAGE.getMonthCalcBoltName(), new VoltageMonthDataCalcBolt(props, false),
                voltageMonthCalcNum == 0 ? defaultTaskNum : voltageMonthCalcNum)
                .customGrouping(ComponentNameEnum.VOLTAGE.getDayCalcBoltName(), StreamKey.VoltageStream.VOLTAGE_MONTH_BOLT_F, new IndexHashGrouping(1));
        builder.setBolt(ComponentNameEnum.VOLTAGE.getMonthPersistBoltName(), new VoltageMonthDataPersistBolt(props, false),
                voltageMonthPersistNum == 0 ? defaultTaskNum : voltageMonthPersistNum)
                .customGrouping(ComponentNameEnum.VOLTAGE.getMonthCalcBoltName(), StreamKey.VoltageStream.VOLTAGE_MONTH_BOLT_S, new IndexHashGrouping(1));
        //year
        builder.setBolt(ComponentNameEnum.VOLTAGE.getYearCalcBoltName(), new VoltageYearDataCalcBolt(props, false),
                voltageYearCalcNum == 0 ? defaultTaskNum : voltageYearCalcNum)
                .customGrouping(ComponentNameEnum.VOLTAGE.getDayCalcBoltName(), StreamKey.VoltageStream.VOLTAGE_YEAR_BOLT_F, new IndexHashGrouping(1));
        builder.setBolt(ComponentNameEnum.VOLTAGE.getYearPersistBoltName(), new VoltageYearDataPersistBolt(props, false),
                voltageYearPersistNum == 0 ? defaultTaskNum : voltageYearPersistNum)
                .customGrouping(ComponentNameEnum.VOLTAGE.getYearCalcBoltName(), StreamKey.VoltageStream.VOLTAGE_YEAR_BOLT_S, new IndexHashGrouping(1));

    }

    public void buildMileageTopo(Properties props, TopologyBuilder builder, int defaultTaskNum) {
        int mileageDayCalcNum = Integer.valueOf( props.getProperty("storm.task.mileage.day.calc.num","1"));
        int mileageDayPersistNum = Integer.valueOf(props.getProperty("storm.task.day.mileage.persist.num","1"));
        int mileageDayFirstPersistNum = Integer.valueOf( props.getProperty("storm.task.day.mileage.first.persist.num","1"));
        int mileageMonthCalcNum = Integer.valueOf( props.getProperty("storm.task.mileage.month.calc.num","1"));
        int mileageMonthPersistNum = Integer.valueOf(props.getProperty("storm.task.month.mileage.persist.num","1"));
        int mileageYearCalcNum = Integer.valueOf( props.getProperty("storm.task.mileage.year.calc.num","1"));
        int mileageYearPersistNum = Integer.valueOf(props.getProperty("storm.task.year.mileage.persist.num","1"));

        builder.setBolt(ComponentNameEnum.MILEAGE.getDayCalcBoltName(), new MileageDayDataCalcBolt(props,false),
                mileageDayCalcNum==0?defaultTaskNum:mileageDayCalcNum)
                .customGrouping(ComponentNameEnum.MILEAGE.getSpoutNameOther(), StreamKey.MileageStream.MILEAGE_HOUR_BOLT_F, new IndexHashGrouping(1));

        builder.setBolt(ComponentNameEnum.MILEAGE.getDayPersistBoltName(), new MileageDayDataPersistBolt(props, false),
                mileageDayPersistNum == 0 ? defaultTaskNum : mileageDayPersistNum)
                .customGrouping(ComponentNameEnum.MILEAGE.getDayCalcBoltName(), StreamKey.MileageStream.MILEAGE_DAY_BOLT_S, new IndexHashGrouping(1))
                .customGrouping(ComponentNameEnum.ZONE_SCHEDULED.getDayCalcBoltName(), StreamKey.MileageStream.MILEAGE_SCHEDULE_DAY_BOLT_S, new IndexHashGrouping(1))
                .customGrouping(ComponentNameEnum.DORMANCY_SCHEDULED.getDayCalcBoltName(), StreamKey.MileageStream.MILEAGE_DORMANCY_DAY_BOLT_S, new IndexHashGrouping(1));

        builder.setBolt(ComponentNameEnum.MILEAGE.getDayFirstSourceBoltName(), new MileageDayFirstDataPBolt(props, false),
                mileageDayFirstPersistNum == 0 ? defaultTaskNum : mileageDayFirstPersistNum)
                .customGrouping(ComponentNameEnum.MILEAGE.getDayCalcBoltName(), StreamKey.MileageStream.MILEAGE_DAY_BOLT_FIRST_DATA, new IndexHashGrouping(1));

        //month
        builder.setBolt(ComponentNameEnum.MILEAGE.getMonthCalcBoltName(), new MileageMonthDataCalcBolt(props, false),
                mileageMonthCalcNum == 0 ? defaultTaskNum : mileageMonthCalcNum)
                .customGrouping(ComponentNameEnum.MILEAGE.getDayCalcBoltName(), StreamKey.MileageStream.MILEAGE_MONTH_BOLT_F, new IndexHashGrouping(1));
        builder.setBolt(ComponentNameEnum.MILEAGE.getMonthPersistBoltName(), new MileageMonthDataPersistBolt(props, false),
                mileageMonthPersistNum == 0 ? defaultTaskNum : mileageMonthPersistNum)
                .customGrouping(ComponentNameEnum.MILEAGE.getMonthCalcBoltName(), StreamKey.MileageStream.MILEAGE_MONTH_BOLT_S, new IndexHashGrouping(1));
        //year
        builder.setBolt(ComponentNameEnum.MILEAGE.getYearCalcBoltName(), new MileageYearDataCalcBolt(props, false),
                mileageYearCalcNum == 0 ? defaultTaskNum : mileageYearCalcNum)
                .customGrouping(ComponentNameEnum.MILEAGE.getDayCalcBoltName(), StreamKey.MileageStream.MILEAGE_YEAR_BOLT_F, new IndexHashGrouping(1));
        builder.setBolt(ComponentNameEnum.MILEAGE.getYearPersistBoltName(), new MileageYearDataPersistBolt(props, false),
                mileageYearPersistNum == 0 ? defaultTaskNum : mileageYearPersistNum)
                .customGrouping(ComponentNameEnum.MILEAGE.getYearCalcBoltName(), StreamKey.MileageStream.MILEAGE_YEAR_BOLT_S, new IndexHashGrouping(1));

    }

    @Override
    public void buildAllTop(Properties props, TopologyBuilder builder, String bolt) {
        super.buildAllTop(props, builder, bolt);
        int defaultTaskNum = Integer.valueOf(props.getProperty("storm.task.defualtNum", "1")).intValue();
        if(bolt.contains("mileage"))
            buildMileageTopo(props, builder, defaultTaskNum);
    }
}

package cst.jstorm.hour.topology.topocreate;

import backtype.storm.topology.TopologyBuilder;
import com.cst.jstorm.commons.stream.constants.ComponentNameEnum;
import com.cst.jstorm.commons.stream.constants.StreamKey;
import com.cst.jstorm.commons.stream.custom.BoltCreateAdapter;
import cst.jstorm.hour.bolt.am.AmHourDataCalcBolt;
import cst.jstorm.hour.bolt.am.AmHourDataPersistBolt;
import cst.jstorm.hour.bolt.am.AmHourFirstDataPBolt;
import cst.jstorm.hour.bolt.de.DeHourDataCalcBolt;
import cst.jstorm.hour.bolt.de.DeHourDataPersistBolt;
import cst.jstorm.hour.bolt.de.DeHourFirstDataPBolt;
import cst.jstorm.hour.bolt.gps.GpsHourDataCalcBolt;
import cst.jstorm.hour.bolt.gps.GpsHourDataPersistBolt;
import cst.jstorm.hour.bolt.gps.GpsHourFirstDataPBolt;
import cst.jstorm.hour.bolt.dispatch.DispatchHourCalcBolt;
import cst.jstorm.hour.bolt.obd.ObdHourDataCalcBolt;
import cst.jstorm.hour.bolt.obd.ObdHourDataPersistBolt;
import cst.jstorm.hour.bolt.obd.ObdHourFirstDataPBolt;
import cst.jstorm.hour.bolt.trace.TraceHourDataCalcBolt;
import cst.jstorm.hour.bolt.trace.TraceHourDataPersistBolt;
import cst.jstorm.hour.bolt.trace.TraceHourFirstDataPBolt;
import cst.jstorm.hour.bolt.tracedelete.TraceDeleteHourDataCalcBolt;
import cst.jstorm.hour.bolt.tracedelete.TraceDeleteHourDataPersistBolt;
import cst.jstorm.hour.bolt.tracedelete.TraceDeleteHourFirstDataPBolt;
import cst.jstorm.hour.bolt.voltage.VoltageHourDataCalcBolt;
import cst.jstorm.hour.bolt.voltage.VoltageHourDataPersistBolt;
import cst.jstorm.hour.bolt.voltage.VoltageHourFirstDataPBolt;
import storm.trident.partition.IndexHashGrouping;

import java.util.Properties;

/**
 * @author Johnney.Chiu
 * create on 2018/5/14 15:23
 * @Description
 * @title
 */
public class HourGdcp3TopoBoltPartDispersed extends BoltCreateAdapter {


    @Override
    public void buildObdTopo(Properties props, TopologyBuilder builder, int defaultTaskNum) {
        int obdHourCalcNum = Integer.valueOf(props.getProperty("storm.task.obd.hour.calc.num","1"));
        int obdHourPersistNum = Integer.valueOf(props.getProperty("storm.task.hour.obd.persist.num","1"));
        int obdHourFirstPersistNum = Integer.valueOf(props.getProperty("storm.task.hour.obd.first.persist.num","1"));
        int obdDayCalcNum = Integer.valueOf(props.getProperty("storm.task.obd.day.calc.num","1"));
        int obdDayPersistNum = Integer.valueOf(props.getProperty("storm.task.day.obd.persist.num","1"));
        //obd小时数据计算
        builder.setBolt(ComponentNameEnum.OBD.getHourCalcBoltName(), new ObdHourDataCalcBolt(props,false),
                obdHourCalcNum==0?defaultTaskNum:obdHourCalcNum)
                .customGrouping(ComponentNameEnum.OBD.getSpoutName(), StreamKey.ObdStream.OBD_GDCP3_HOUR_BOLT_F, new IndexHashGrouping(1))
                .customGrouping(ComponentNameEnum.ELECTRIC_OBD.getSpoutName(), StreamKey.ElectricObdStream.ELECTRIC_GDCP3_OBD_HOUR_BOLT_F, new IndexHashGrouping(1));


        builder.setBolt(ComponentNameEnum.OBD.getHourPersistBoltName(), new ObdHourDataPersistBolt(props, false),
                obdHourPersistNum == 0 ? defaultTaskNum : obdHourPersistNum)
                .customGrouping(ComponentNameEnum.OBD.getHourCalcBoltName(), StreamKey.ObdStream.OBD_HOUR_BOLT_S, new IndexHashGrouping(1));

        builder.setBolt(ComponentNameEnum.OBD.getFirstSourceBoltName(), new ObdHourFirstDataPBolt(props, false),
                obdHourFirstPersistNum == 0 ? defaultTaskNum : obdHourFirstPersistNum)
                .customGrouping(ComponentNameEnum.OBD.getHourCalcBoltName(), StreamKey.ObdStream.OBD_HOUR_BOLT_FIRST_DATA, new IndexHashGrouping(1));



    }

    @Override
    public void buildGpsTopo(Properties props, TopologyBuilder builder, int defaultTaskNum) {

        int gpsHourCalcNum = Integer.valueOf( props.getProperty("storm.task.gps.hour.calc.num","1"));
        int gpsHourPersistNum = Integer.valueOf( props.getProperty("storm.task.hour.gps.persist.num","1"));
        int gpsHourFirstPersistNum = Integer.valueOf( props.getProperty("storm.task.hour.gps.first.persist.num","1"));
        builder.setBolt(ComponentNameEnum.GPS.getHourCalcBoltName(), new GpsHourDataCalcBolt(props,false),
                gpsHourCalcNum==0?defaultTaskNum:gpsHourCalcNum)
                .customGrouping(ComponentNameEnum.GPS.getSpoutName(),StreamKey.GpsStream.GPS_GDCP3_HOUR_BOLT_F, new IndexHashGrouping(1));

        builder.setBolt(ComponentNameEnum.GPS.getHourPersistBoltName(), new GpsHourDataPersistBolt(props, false),
                gpsHourPersistNum == 0 ? defaultTaskNum : gpsHourPersistNum)
                .customGrouping(ComponentNameEnum.GPS.getHourCalcBoltName(), StreamKey.GpsStream.GPS_HOUR_BOLT_S, new IndexHashGrouping(1));
        builder.setBolt(ComponentNameEnum.GPS.getFirstSourceBoltName(), new GpsHourFirstDataPBolt(props, false),
                gpsHourFirstPersistNum == 0 ? defaultTaskNum : gpsHourFirstPersistNum)
                .customGrouping(ComponentNameEnum.GPS.getHourCalcBoltName(), StreamKey.GpsStream.GPS_HOUR_BOLT_FIRST_DATA, new IndexHashGrouping(1));

    }

    @Override
    public void buildAmTopo(Properties props, TopologyBuilder builder, int defaultTaskNum) {
        int amHourCalcNum = Integer.valueOf( props.getProperty("storm.task.am.hour.calc.num","1"));
        int amHourPersistNum = Integer.valueOf( props.getProperty("storm.task.hour.am.persist.num","1"));
        int amHourFirstPersistNum = Integer.valueOf( props.getProperty("storm.task.hour.am.first.persist.num","1"));

        builder.setBolt(ComponentNameEnum.AM.getHourCalcBoltName(), new AmHourDataCalcBolt(props,false),
                amHourCalcNum==0?defaultTaskNum:amHourCalcNum)
                .customGrouping(ComponentNameEnum.DE.getSpoutName(),StreamKey.AmStream.AM_GDCP3_HOUR_BOLT_F, new IndexHashGrouping(1));
        builder.setBolt(ComponentNameEnum.AM.getHourPersistBoltName(), new AmHourDataPersistBolt(props, false),
                amHourPersistNum == 0 ? defaultTaskNum : amHourPersistNum)
                .customGrouping(ComponentNameEnum.AM.getHourCalcBoltName(), StreamKey.AmStream.AM_HOUR_BOLT_S, new IndexHashGrouping(1));
        builder.setBolt(ComponentNameEnum.AM.getFirstSourceBoltName(), new AmHourFirstDataPBolt(props, false),
                amHourFirstPersistNum == 0 ? defaultTaskNum : amHourFirstPersistNum)
                .customGrouping(ComponentNameEnum.AM.getHourCalcBoltName(), StreamKey.AmStream.AM_HOUR_BOLT_FIRST_DATA, new IndexHashGrouping(1));

    }

    @Override
    public void buildDeTopo(Properties props, TopologyBuilder builder, int defaultTaskNum) {
        int deHourCalcNum = Integer.valueOf(props.getProperty("storm.task.de.hour.calc.num","1"));
        int deHourPersistNum = Integer.valueOf( props.getProperty("storm.task.hour.de.persist.num","1"));
        int deHourFirstPersistNum = Integer.valueOf( props.getProperty("storm.task.hour.de.first.persist.num","1"));

        builder.setBolt(ComponentNameEnum.DE.getHourCalcBoltName(), new DeHourDataCalcBolt(props,false),
                deHourCalcNum==0?defaultTaskNum:deHourCalcNum)
                .customGrouping(ComponentNameEnum.DE.getSpoutName(),StreamKey.DeStream.DE_GDCP3_HOUR_BOLT_F, new IndexHashGrouping(1));

        builder.setBolt(ComponentNameEnum.DE.getHourPersistBoltName(), new DeHourDataPersistBolt(props, false),
                deHourPersistNum == 0 ? defaultTaskNum : deHourPersistNum)
                .customGrouping(ComponentNameEnum.DE.getHourCalcBoltName(), StreamKey.DeStream.DE_HOUR_BOLT_S, new IndexHashGrouping(1));
        builder.setBolt(ComponentNameEnum.DE.getFirstSourceBoltName(), new DeHourFirstDataPBolt(props, false),
                deHourFirstPersistNum == 0 ? defaultTaskNum : deHourFirstPersistNum)
                .customGrouping(ComponentNameEnum.DE.getHourCalcBoltName(), StreamKey.DeStream.DE_HOUR_BOLT_FIRST_DATA, new IndexHashGrouping(1));

    }

    @Override
    public void buildIntegratedTopo(Properties props, TopologyBuilder builder, int defaultTaskNum) {

        int integratedHourDispatchNum = Integer.valueOf(props.getProperty("storm.task.integrated.hour.dispatch.num","1"));

        builder.setBolt(ComponentNameEnum.INTEGRATED.getHourDispatchBoltName(), new DispatchHourCalcBolt(props, false),
                integratedHourDispatchNum == 0 ? defaultTaskNum : integratedHourDispatchNum)
                .customGrouping(ComponentNameEnum.DE.getHourPersistBoltName(), StreamKey.ObdStream.DE_HOUR_PERSIST_S, new IndexHashGrouping(1))
                .customGrouping(ComponentNameEnum.OBD.getHourPersistBoltName(), StreamKey.ObdStream.OBD_HOUR_PERSIST_S, new IndexHashGrouping(1));
    }

    @Override
    public void buildTraceTopo(Properties props, TopologyBuilder builder, int defaultTaskNum) {
        int traceHourCalcNum = Integer.valueOf( props.getProperty("storm.task.trace.hour.calc.num","1"));
        int traceHourPersistNum = Integer.valueOf( props.getProperty("storm.task.hour.trace.persist.num","1"));
        int traceHourFirstPersistNum = Integer.valueOf( props.getProperty("storm.task.hour.trace.first.persist.num","1"));


        builder.setBolt(ComponentNameEnum.TRACE.getHourCalcBoltName(), new TraceHourDataCalcBolt(props,false),
                traceHourCalcNum==0?defaultTaskNum:traceHourCalcNum)
                .customGrouping(ComponentNameEnum.TRACE.getSpoutName(),StreamKey.TraceStream.TRACE_GDCP3_HOUR_BOLT_F, new IndexHashGrouping(1));
        builder.setBolt(ComponentNameEnum.TRACE.getHourPersistBoltName(), new TraceHourDataPersistBolt(props, false),
                traceHourPersistNum == 0 ? defaultTaskNum : traceHourPersistNum)
                .customGrouping(ComponentNameEnum.TRACE.getHourCalcBoltName(), StreamKey.TraceStream.TRACE_HOUR_BOLT_S, new IndexHashGrouping(1));
        builder.setBolt(ComponentNameEnum.TRACE.getFirstSourceBoltName(), new TraceHourFirstDataPBolt(props, false),
                traceHourFirstPersistNum == 0 ? defaultTaskNum : traceHourFirstPersistNum)
                .customGrouping(ComponentNameEnum.TRACE.getHourCalcBoltName(), StreamKey.TraceStream.TRACE_HOUR_BOLT_FIRST_DATA, new IndexHashGrouping(1));

    }

    @Override
    public void buildTraceDeleteTopo(Properties props, TopologyBuilder builder, int defaultTaskNum) {
        int traceDeleteHourCalcNum = Integer.valueOf( props.getProperty("storm.task.tracedelete.hour.calc.num","1"));
        int traceDeleteHourPersistNum = Integer.valueOf( props.getProperty("storm.task.hour.tracedelete.persist.num","1"));
        int traceDeleteHourFirstPersistNum = Integer.valueOf( props.getProperty("storm.task.hour.tracedelete.first.persist.num","1"));


        builder.setBolt(ComponentNameEnum.TRACE_DELETE.getHourCalcBoltName(), new TraceDeleteHourDataCalcBolt(props,false),
                traceDeleteHourCalcNum==0?defaultTaskNum:traceDeleteHourCalcNum)
                .customGrouping(ComponentNameEnum.TRACE_DELETE.getSpoutName(),StreamKey.TraceDeleteStream.TRACE_DELETE_GDCP3_HOUR_BOLT_F, new IndexHashGrouping(1));
        builder.setBolt(ComponentNameEnum.TRACE_DELETE.getHourPersistBoltName(), new TraceDeleteHourDataPersistBolt(props, false),
                traceDeleteHourPersistNum == 0 ? defaultTaskNum : traceDeleteHourPersistNum)
                .customGrouping(ComponentNameEnum.TRACE_DELETE.getHourCalcBoltName(), StreamKey.TraceDeleteStream.TRACE_DELETE_HOUR_BOLT_S, new IndexHashGrouping(1));
        builder.setBolt(ComponentNameEnum.TRACE_DELETE.getFirstSourceBoltName(), new TraceDeleteHourFirstDataPBolt(props, false),
                traceDeleteHourFirstPersistNum == 0 ? defaultTaskNum : traceDeleteHourFirstPersistNum)
                .customGrouping(ComponentNameEnum.TRACE_DELETE.getHourCalcBoltName(), StreamKey.TraceDeleteStream.TRACE_DELETE_HOUR_BOLT_FIRST_DATA, new IndexHashGrouping(1));


    }

    @Override
    public void buildVoltageTopo(Properties props, TopologyBuilder builder, int defaultTaskNum) {
        int voltageHourCalcNum = Integer.valueOf( props.getProperty("storm.task.voltage.hour.calc.num","1"));
        int voltageHourPersistNum = Integer.valueOf( props.getProperty("storm.task.hour.voltage.persist.num","1"));
        int voltageHourNoDelayPersistNum = Integer.valueOf( props.getProperty("storm.task.hour.voltage.first.persist.num","1"));
        builder.setBolt(ComponentNameEnum.VOLTAGE.getHourCalcBoltName(), new VoltageHourDataCalcBolt(props,false),
                voltageHourCalcNum==0?defaultTaskNum:voltageHourCalcNum)
                .customGrouping(ComponentNameEnum.OTHER.getSpoutName(),StreamKey.VoltageStream.VOLTAGE_GDCP3_HOUR_BOLT_F, new IndexHashGrouping(1));

        builder.setBolt(ComponentNameEnum.VOLTAGE.getHourPersistBoltName(), new VoltageHourDataPersistBolt(props, false),
                voltageHourPersistNum == 0 ? defaultTaskNum : voltageHourPersistNum)
                .customGrouping(ComponentNameEnum.VOLTAGE.getHourCalcBoltName(), StreamKey.VoltageStream.VOLTAGE_HOUR_BOLT_S, new IndexHashGrouping(1));
        builder.setBolt(ComponentNameEnum.VOLTAGE.getFirstSourceBoltName(), new VoltageHourFirstDataPBolt(props, false),
                voltageHourNoDelayPersistNum == 0 ? defaultTaskNum : voltageHourNoDelayPersistNum)
                .customGrouping(ComponentNameEnum.VOLTAGE.getHourCalcBoltName(), StreamKey.VoltageStream.VOLTAGE_HOUR_BOLT_FIRST_DATA, new IndexHashGrouping(1));
    }


    @Override
    public void buildAllTop(Properties props, TopologyBuilder builder, String bolt) {
        int defaultTaskNum = Integer.valueOf(props.getProperty("storm.task.defualtNum","1"));

        if(bolt.contains("obd"))
            buildObdTopo(props,builder,defaultTaskNum);
        if(bolt.contains("gps"))
            buildGpsTopo(props,builder,defaultTaskNum);
        if(bolt.contains("de")) {
            buildDeTopo(props, builder, defaultTaskNum);
            buildAmTopo(props, builder, defaultTaskNum);
        }

        if(bolt.contains("trace"))
            buildTraceTopo(props,builder,defaultTaskNum);
        if(bolt.contains("trace_delete"))
            buildTraceDeleteTopo(props,builder,defaultTaskNum);

        if(bolt.contains("integrated"))
            buildIntegratedTopo(props, builder, defaultTaskNum);
        if(bolt.contains("other"))
            buildVoltageTopo(props,builder,defaultTaskNum);

    }
}

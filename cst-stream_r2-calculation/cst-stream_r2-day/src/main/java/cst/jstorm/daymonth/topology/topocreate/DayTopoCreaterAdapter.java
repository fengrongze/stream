package cst.jstorm.daymonth.topology.topocreate;


import backtype.storm.topology.TopologyBuilder;
import com.cst.jstorm.commons.stream.constants.ComponentNameEnum;
import com.cst.jstorm.commons.stream.custom.BoltCreateAdapter;
import com.cst.jstorm.commons.utils.TopoCreateAdapter;
import cst.jstorm.daymonth.spout.*;

import java.util.Properties;

/**
 * @author Johnney.Chiu
 * create on 2018/5/23 11:56
 * @Description
 * @title
 */
public class DayTopoCreaterAdapter extends TopoCreateAdapter {

    protected BoltCreateAdapter boltCreateAdapter;

    public DayTopoCreaterAdapter(TopologyBuilder builder, Properties props, BoltCreateAdapter boltCreateAdapter) {
        super(builder, props);
        this.boltCreateAdapter = boltCreateAdapter;
    }

    @Override
    protected void createSpout(String spout) {
        int defaultTaskNum = Integer.valueOf(props.getProperty("storm.task.defualtNum","1"));
        int obdSpoutNum = Integer.valueOf( props.getProperty("storm.task.obd.spout.num","1"));
        int electricObdSpoutNum = Integer.valueOf( props.getProperty("storm.task.electric.obd.spout.num","1"));
        int gpsSpoutNum = Integer.valueOf(props.getProperty("storm.task.gps.spout.num","1"));
        int deSpoutNum = Integer.valueOf(props.getProperty("storm.task.de.spout.num","1"));
        int amSpoutNum = Integer.valueOf(props.getProperty("storm.task.am.spout.num","1"));
        int traceSpoutNum = Integer.valueOf(props.getProperty("storm.task.trace.spout.num","1"));
        int traceDeleteSpoutNum = Integer.valueOf(props.getProperty("storm.task.tracedelete.spout.num","1"));
        int voltageSpoutNum = Integer.valueOf(props.getProperty("storm.task.voltage.spout.num","1"));
        int mileageSpoutNum = Integer.valueOf(props.getProperty("storm.task.mileage.spout.num","1"));
        int scheduleSpoutNum = Integer.valueOf(props.getProperty("storm.task.day.schedule.spout.num","1"));
        int dormancySpoutNum = Integer.valueOf(props.getProperty("storm.task.day.dormancy.spout.num","1"));

        //spout 的创建
        if (spout.contains("obd")) {

            builder.setSpout(ComponentNameEnum.OBD.getSpoutNameOther(), new ObdSpout(props, false), obdSpoutNum == 0
                    ? defaultTaskNum : obdSpoutNum);
            builder.setSpout(ComponentNameEnum.ELECTRIC_OBD.getSpoutNameOther(), new ElectricObdSpout(props, false), electricObdSpoutNum == 0
                    ? defaultTaskNum : electricObdSpoutNum);
        }
        if(spout.contains("gps"))
            builder.setSpout(ComponentNameEnum.GPS.getSpoutNameOther(), new GpsSpout(props,false), gpsSpoutNum ==0
                ?defaultTaskNum:gpsSpoutNum);
        if(spout.contains("am"))
            builder.setSpout(ComponentNameEnum.AM.getSpoutNameOther(), new AmSpout(props,false), amSpoutNum ==0
                ?defaultTaskNum:amSpoutNum);
        if(spout.contains("de"))
            builder.setSpout(ComponentNameEnum.DE.getSpoutNameOther(), new DeSpout(props,false), deSpoutNum ==0
                ?defaultTaskNum:deSpoutNum);
        if(spout.contains("trace"))
            builder.setSpout(ComponentNameEnum.TRACE.getSpoutNameOther(), new TraceSpout(props,false), traceSpoutNum==0
                ?defaultTaskNum:traceSpoutNum);
        if(spout.contains("trace_delete"))
            builder.setSpout(ComponentNameEnum.TRACE_DELETE.getSpoutNameOther(), new TraceDeleteSpout(props,false), traceDeleteSpoutNum==0
                ?defaultTaskNum:traceDeleteSpoutNum);
        if(spout.contains("voltage"))
            builder.setSpout(ComponentNameEnum.VOLTAGE.getSpoutNameOther(), new VoltageSpout(props,false), voltageSpoutNum==0
                ?defaultTaskNum:voltageSpoutNum);
        if (spout.contains("before")) {

            builder.setSpout(ComponentNameEnum.ZONE_SCHEDULED.getSpoutNameOther(), new DayScheduleTimerSpout(props, false), scheduleSpoutNum == 0
                    ? defaultTaskNum : scheduleSpoutNum);
            builder.setSpout(ComponentNameEnum.DORMANCY_SCHEDULED.getSpoutNameOther(), new DormantSpout(props, false), dormancySpoutNum == 0
                    ? defaultTaskNum : dormancySpoutNum);
        }
        if(spout.contains("mileage"))
            builder.setSpout(ComponentNameEnum.MILEAGE.getSpoutNameOther(), new MileageSpout(props,false), mileageSpoutNum==0
                    ?defaultTaskNum:mileageSpoutNum);
    }

    @Override
    protected void createBolt(String bolt) {
        boltCreateAdapter.buildAllTop(props,builder,bolt);
    }
}

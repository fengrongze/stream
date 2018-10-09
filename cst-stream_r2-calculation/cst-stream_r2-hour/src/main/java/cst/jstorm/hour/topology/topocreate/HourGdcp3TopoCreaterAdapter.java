package cst.jstorm.hour.topology.topocreate;


import backtype.storm.topology.TopologyBuilder;
import com.cst.jstorm.commons.stream.constants.ComponentNameEnum;
import com.cst.jstorm.commons.stream.custom.BoltCreateAdapter;
import com.cst.jstorm.commons.utils.TopoCreateAdapter;
import cst.jstorm.hour.spout.*;
import cst.jstorm.hour.spout.gdcp3.*;

import java.util.Properties;

/**
 * @author Johnney.Chiu
 * create on 2018/5/23 11:56
 * @Description
 * @title
 */
public class HourGdcp3TopoCreaterAdapter extends TopoCreateAdapter {

    protected BoltCreateAdapter boltCreateAdapter;

    public HourGdcp3TopoCreaterAdapter(TopologyBuilder builder, Properties props, BoltCreateAdapter boltCreateAdapter) {
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
        int traceSpoutNum = Integer.valueOf(props.getProperty("storm.task.trace.spout.num","1"));
        int traceDeleteSpoutNum = Integer.valueOf(props.getProperty("storm.task.tracedelete.spout.num","1"));
        int otherSpoutNum = Integer.valueOf(props.getProperty("storm.task.other.spout.num","1"));
        //spout 的创建
        if (spout.contains("obd")) {

            builder.setSpout(ComponentNameEnum.OBD.getSpoutName(), new ObdGdcp3Spout(props, false), obdSpoutNum == 0
                    ? defaultTaskNum : obdSpoutNum);
            builder.setSpout(ComponentNameEnum.ELECTRIC_OBD.getSpoutName(), new ElectricObdGdcp3Spout(props, false), electricObdSpoutNum == 0
                    ? defaultTaskNum : electricObdSpoutNum);
        }
        if(spout.contains("gps"))
            builder.setSpout(ComponentNameEnum.GPS.getSpoutName(), new GpsGdcp3Spout(props,false), gpsSpoutNum ==0
                ?defaultTaskNum:gpsSpoutNum);

        if(spout.contains("de"))
            builder.setSpout(ComponentNameEnum.DE.getSpoutName(), new DeGdcp3Spout(props,false), deSpoutNum ==0
                ?defaultTaskNum:deSpoutNum);
        if(spout.contains("trace"))
            builder.setSpout(ComponentNameEnum.TRACE.getSpoutName(), new TraceGdcp3Spout(props,false), traceSpoutNum==0
                ?defaultTaskNum:traceSpoutNum);
        if(spout.contains("trace_delete"))
            builder.setSpout(ComponentNameEnum.TRACE_DELETE.getSpoutName(), new TraceDeleteGdcp3Spout(props,false), traceDeleteSpoutNum==0
                ?defaultTaskNum:traceDeleteSpoutNum);
        if(spout.contains("other"))
            builder.setSpout(ComponentNameEnum.OTHER.getSpoutName(), new OtherGdcp3Spout(props,false), otherSpoutNum==0
                ?defaultTaskNum:otherSpoutNum);

    }

    @Override
    protected void createBolt(String bolt) {
        boltCreateAdapter.buildAllTop(props,builder,bolt);
    }
}

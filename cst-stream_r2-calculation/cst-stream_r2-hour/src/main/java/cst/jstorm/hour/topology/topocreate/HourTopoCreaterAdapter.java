package cst.jstorm.hour.topology.topocreate;


import backtype.storm.topology.TopologyBuilder;
import com.cst.jstorm.commons.stream.constants.ComponentNameEnum;
import com.cst.jstorm.commons.stream.custom.BoltCreateAdapter;
import com.cst.jstorm.commons.utils.TopoCreateAdapter;
import cst.jstorm.hour.spout.*;

import java.util.Map;
import java.util.Properties;

/**
 * @author Johnney.Chiu
 * create on 2018/5/23 11:56
 * @Description
 * @title
 */
public class HourTopoCreaterAdapter extends TopoCreateAdapter {

    protected BoltCreateAdapter boltCreateAdapter;

    public HourTopoCreaterAdapter(TopologyBuilder builder, Properties props, BoltCreateAdapter boltCreateAdapter) {
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
        //spout 的创建
        if (spout.contains("obd")) {

            builder.setSpout(ComponentNameEnum.OBD.getSpoutName(), new ObdSpout(props, false), obdSpoutNum == 0
                    ? defaultTaskNum : obdSpoutNum);
            builder.setSpout(ComponentNameEnum.ELECTRIC_OBD.getSpoutName(), new ElectricObdSpout(props, false), electricObdSpoutNum == 0
                    ? defaultTaskNum : electricObdSpoutNum);
        }
        if(spout.contains("gps"))
            builder.setSpout(ComponentNameEnum.GPS.getSpoutName(), new GpsSpout(props,false), gpsSpoutNum ==0
                ?defaultTaskNum:gpsSpoutNum);
        if(spout.contains("am"))
            builder.setSpout(ComponentNameEnum.AM.getSpoutName(), new AmSpout(props,false), amSpoutNum ==0
                ?defaultTaskNum:amSpoutNum);
        if(spout.contains("de"))
            builder.setSpout(ComponentNameEnum.DE.getSpoutName(), new DeSpout(props,false), deSpoutNum ==0
                ?defaultTaskNum:deSpoutNum);
        if(spout.contains("trace"))
            builder.setSpout(ComponentNameEnum.TRACE.getSpoutName(), new TraceSpout(props,false), traceSpoutNum==0
                ?defaultTaskNum:traceSpoutNum);
        if(spout.contains("trace_delete"))
            builder.setSpout(ComponentNameEnum.TRACE_DELETE.getSpoutName(), new TraceDeleteSpout(props,false), traceDeleteSpoutNum==0
                ?defaultTaskNum:traceDeleteSpoutNum);
        if(spout.contains("voltage"))
            builder.setSpout(ComponentNameEnum.VOLTAGE.getSpoutName(), new VoltageSpout(props,false), voltageSpoutNum==0
                ?defaultTaskNum:voltageSpoutNum);
        if(spout.contains("mileage"))
            builder.setSpout(ComponentNameEnum.MILEAGE.getSpoutName(), new MileageSpout(props,false), mileageSpoutNum==0
                    ?defaultTaskNum:mileageSpoutNum);
    }

    @Override
    protected void createBolt(String bolt) {
        boltCreateAdapter.buildAllTop(props,builder,bolt);
    }
}

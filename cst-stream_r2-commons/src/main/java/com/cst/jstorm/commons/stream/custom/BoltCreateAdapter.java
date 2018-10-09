package com.cst.jstorm.commons.stream.custom;

import backtype.storm.topology.TopologyBuilder;

import java.util.Properties;

/**
 * @author Johnney.Chiu
 * create on 2018/5/14 15:25
 * @Description
 * @title
 */
public abstract class BoltCreateAdapter {

    public  void buildScheduleTopo(Properties props, TopologyBuilder builder, int defaultTaskNum){}

    public  void buildDormancyTopo(Properties props, TopologyBuilder builder, int defaultTaskNum){}

    public  void buildObdTopo(Properties props, TopologyBuilder builder, int defaultTaskNum){}

    public  void buildGpsTopo(Properties props, TopologyBuilder builder, int defaultTaskNum){}

    public  void buildAmTopo(Properties props, TopologyBuilder builder, int defaultTaskNum){}

    public  void buildDeTopo(Properties props, TopologyBuilder builder, int defaultTaskNum){}

    public  void buildIntegratedTopo(Properties props, TopologyBuilder builder, int defaultTaskNum){}

    public  void buildTraceTopo(Properties props, TopologyBuilder builder, int defaultTaskNum){}

    public  void buildTraceDeleteTopo(Properties props, TopologyBuilder builder, int defaultTaskNum){}

    public  void buildVoltageTopo(Properties props, TopologyBuilder builder, int defaultTaskNum){}


    public void buildAllTop(Properties props, TopologyBuilder builder,String bolt){
        int defaultTaskNum = Integer.valueOf(props.getProperty("storm.task.defualtNum","1"));
        if (bolt.contains("before")) {
            buildScheduleTopo(props, builder, defaultTaskNum);
            buildDormancyTopo(props, builder, defaultTaskNum);
        }
        if(bolt.contains("obd"))
            buildObdTopo(props,builder,defaultTaskNum);
        if(bolt.contains("gps"))
            buildGpsTopo(props,builder,defaultTaskNum);
        if(bolt.contains("de"))
            buildDeTopo(props,builder,defaultTaskNum);
        if(bolt.contains("am"))
            buildAmTopo(props,builder,defaultTaskNum);
        if(bolt.contains("trace"))
            buildTraceTopo(props,builder,defaultTaskNum);
        if(bolt.contains("trace_delete"))
            buildTraceDeleteTopo(props,builder,defaultTaskNum);
        if(bolt.contains("voltage"))
            buildVoltageTopo(props,builder,defaultTaskNum);
        if(bolt.contains("integrated"))
            buildIntegratedTopo(props, builder, defaultTaskNum);
    }

}

package com.cst.jstorm.commons.utils;

import backtype.storm.topology.TopologyBuilder;

import java.util.Properties;

/**
 * @author Johnney.Chiu
 * create on 2018/5/14 15:25
 * @Description
 * @title
 */
public abstract class TopoCreateAdapter {

    protected TopologyBuilder builder;

    protected Properties props;

    public TopoCreateAdapter(TopologyBuilder builder, Properties props) {
        this.builder = builder;
        this.props = props;
    }

    protected abstract void createSpout(String str);

    protected abstract void createBolt(String str);

    public TopoCreateAdapter createTopo(String str){
        createSpout(str);
        createBolt(str);
        return this;
    }


}

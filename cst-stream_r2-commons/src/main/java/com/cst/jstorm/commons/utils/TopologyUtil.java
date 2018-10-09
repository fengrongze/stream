package com.cst.jstorm.commons.utils;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * cst.jstorm.commons.utils.TopologyUtil
 * 2017年11月7日 上午10:28:00
 * @author lith
 * <p>desc:</p>
 */
public class TopologyUtil {

	private static final Logger logger = LoggerFactory.getLogger(TopologyUtil.class);
	
	/**
	 * auto提交
	 * @param name
	 * @param conf
	 * @param topology
	 */
	public static void autoSubmit(String name, Config conf, StormTopology topology, String type) {
		if ("storm".equals(type)) {
			stormSubmit(name, conf, topology);
		} else {
			conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
			conf.setNumAckers(1);
			conf.setNumWorkers(1);
			LocalSubmit(name, conf, topology);
		}
	}
	
	/**
	 * storm提交
	 * @param name
	 * @param conf
	 * @param topology
	 */
	public static void stormSubmit(String name, Config conf, StormTopology topology) {
		try {
			StormSubmitter.submitTopology(name, conf, topology);
		} catch (AlreadyAliveException | InvalidTopologyException e) {
			logger.error("topology submit failure", e);
		}
	}
	
	/**
	 * local提交
	 * @param name
	 * @param conf
	 * @param topology
	 */
	public static void LocalSubmit(String name, Config conf, StormTopology topology) {
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(name, conf, topology);
	}
	
}

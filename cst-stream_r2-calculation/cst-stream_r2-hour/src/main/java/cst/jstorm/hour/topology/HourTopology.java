package cst.jstorm.hour.topology;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import com.alibaba.jstorm.client.ConfigExtension;
import com.cst.jstorm.commons.utils.AbstractTopologyInitialization;
import com.cst.jstorm.commons.utils.LogbackInitUtil;
import com.cst.jstorm.commons.utils.TopologyUtil;
import cst.jstorm.hour.topology.topocreate.HourGdcp3TopoBoltPartDispersed;
import cst.jstorm.hour.topology.topocreate.HourGdcp3TopoCreaterAdapter;
import cst.jstorm.hour.topology.topocreate.HourTopoBoltPartDispersed;
import cst.jstorm.hour.topology.topocreate.HourTopoCreaterAdapter;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * HourTopology
 * 2017年11月7日 上午11:22:57
 *
 * @author lith
 * <p>desc:流式计算小时天数据拓扑</p>
 */
public class HourTopology extends AbstractTopologyInitialization {

    private static final Logger logger = LoggerFactory.getLogger(HourTopology.class);



    public static void main(String[] args) {
        HourTopology hourTopology = new HourTopology();
        try {
            String param0 = args[0];
            String param1 = args[1];
            if (StringUtils.isBlank(param0) || StringUtils.isBlank(param1)) {
                logger.error("param error check it");
                return;
            }
            Properties props ;
            if (param1.equals("gdcp3")) {
                props = hourTopology.loadProp("gdcp3.properties");
            } else if (param1.equals("old")) {
                props = hourTopology.loadProp("config.properties");
            } else {
                logger.error("param1 error check it");
                return;
            }
            Config conf = hourTopology.initConf(props,param0);
            LogbackInitUtil.changeLogback(props, true);
            logger.info(props.toString());
            TopologyBuilder builder = hourTopology.createBuilder(props,param0,param1);
            TopologyUtil.autoSubmit(hourTopology.getName(props.getProperty("storm.topology.hour.name"),param0), conf, builder.createTopology(), props.getProperty("storm.topology.type"));
        } catch (Exception e) {
            logger.error("hour topology startup failure", e);
        }
    }



    @Override
    protected TopologyBuilder createBuilder(Properties props,String... params) {
        TopologyBuilder builder = new TopologyBuilder();
        String temp="obd,gps,am,de,trace,trace_delete,voltage,integrated,other,mileage";
        if(params[0].contains("obd"))
            temp = "obd";
        if(params[0].contains("gps"))
            temp = "gps";
        if(params[0].contains("integrated"))
            temp = "integrated,am,de,voltage,trace,trace_delete";
        if(params[1].equals("gdcp3")){
            new HourGdcp3TopoCreaterAdapter(builder, props, new HourGdcp3TopoBoltPartDispersed()).createTopo(temp);
        }else if(params[1].equals("old")) {
            new HourTopoCreaterAdapter(builder, props, new HourTopoBoltPartDispersed()).createTopo(temp);
        }
        return builder;
    }

    @Override
    public Config initConf(Properties props,String param) {
        Config config = super.initConf(props,param);

        ConfigExtension.setEnableTopologyClassLoader(config, Boolean.getBoolean(props.getProperty("topology.enable.classloader")));
        //log
        //ConfigExtension.setUserDefinedLogbackConf(config, props.getProperty("cst.storm.config.log","my-logback.xml"));

        //debug
        //config.setDebug(Boolean.getBoolean(props.getProperty("cst.storm.config.log","true")));
        //zookeeper

        //zookeeper跟着系统环境走
        //config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList((props.getProperty("storm.zookeeper.servers")).split(",")));
        //config.put(Config.STORM_ZOOKEEPER_PORT, Integer.parseInt(props.getProperty("storm.zookeeper.port")));
        //config.put(Config.STORM_ZOOKEEPER_ROOT, props.getProperty("storm.zookeeper.root"));
        //config.put(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT, Integer.parseInt(props.getProperty("storm.zookeeper.session.timeout", "3000")));
        //supervisor
        //config.put(Config.SUPERVISOR_CHILDOPTS,props.getProperty("supervisor.childopts"));
        //config.put(Config.SUPERVISOR_WORKER_START_TIMEOUT_SECS, Integer.parseInt(props.getProperty("supervisor.worker.start.timeout.secs", "120")));
        //config.put(Config.SUPERVISOR_HEARTBEAT_FREQUENCY_SECS, Integer.parseInt(props.getProperty("supervisor.heartbeat.frequency.secs", "5")));
        //config.put(Config.SUPERVISOR_MONITOR_FREQUENCY_SECS, Integer.parseInt(props.getProperty("supervisor.monitor.frequency.secs", "3")));
        //worker setting
        if(param.contains("obd"))
            config.setNumWorkers(Integer.parseInt(props.getProperty("topology.hour.obd.workers", "1")));
        if(param.contains("gps"))
            config.setNumWorkers(Integer.parseInt(props.getProperty("topology.hour.gps.workers", "1")));

        if(param.contains("integrated"))
            config.setNumWorkers(Integer.parseInt(props.getProperty("topology.hour.integrated.workers", "1")));

        if(param.contains("all"))
            config.setNumWorkers(Integer.parseInt(props.getProperty("topology.hour.workers", "1")));

        //config.put(Config.SUPERVISOR_WORKER_TIMEOUT_SECS, Integer.parseInt(props.getProperty("supervisor.worker.timeout.secs", "40")));
        //config.put(Config.WORKER_CHILDOPTS, props.getProperty("worker.childopts"));
        //config.put(Config.WORKER_HEARTBEAT_FREQUENCY_SECS,Integer.parseInt( props.getProperty("worker.heartbeat.frequency.secs","3")));

        //task setting
        //config.put(Config.TASK_HEARTBEAT_FREQUENCY_SECS, Integer.parseInt(props.getProperty("task.heartbeat.frequency.secs", "3")));
        //config.put(Config.TASK_REFRESH_POLL_SECS, Integer.parseInt(props.getProperty("task.refresh.poll.secs", "10")));

        //zmq
        //config.put(Config.ZMQ_THREADS, Integer.parseInt(props.getProperty("zmq.threads", "1")));

        //other setting
        //config.put(Config.STORM_THRIFT_TRANSPORT_PLUGIN, props.getProperty("storm.thrift.transport"));
         //config.put(Config.DRPC_CHILDOPTS,props.getProperty("drpc.childopts"));
        config.setNumAckers(Integer.parseInt(props.getProperty("topology.hour.ackers", "1")));
        //消息树的处理时间设置
        //config.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, Integer.parseInt(props.getProperty("topology.message.timeout.secs", "30")));
        //config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, props.getProperty("topology.max.spout.pending"));
        return config;
    }


}

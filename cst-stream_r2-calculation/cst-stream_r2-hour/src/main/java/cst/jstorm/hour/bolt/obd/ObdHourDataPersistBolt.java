package cst.jstorm.hour.bolt.obd;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.cst.stream.common.CstConstants;
import com.cst.stream.common.HBaseTable;
import com.cst.stream.common.HbaseColumn;
import com.cst.stream.common.StreamTypeDefine;
import com.cst.stream.stathour.obd.ObdHourTransfor;
import com.cst.jstorm.commons.stream.constants.*;
import com.cst.jstorm.commons.stream.operations.hbasestrategy.StrategyChoose;
import com.cst.jstorm.commons.utils.HttpURIUtil;
import com.cst.jstorm.commons.utils.LogbackInitUtil;
import com.cst.jstorm.commons.utils.PropertiesUtil;
import com.cst.jstorm.commons.utils.http.HttpUtils;
import com.cst.jstorm.commons.utils.spring.MyApplicationContext;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;

import java.util.Map;
import java.util.Properties;

import static com.cst.jstorm.commons.stream.operations.GeneralPersistUtils.resultPersist;


/**
 * @author Johnney.Chiu
 * create on 2018/5/9 17:09
 * @Description Obd bolt
 * @title
 */
public class ObdHourDataPersistBolt  extends BaseBasicBolt {


    private static final long serialVersionUID = 4180883262628793100L;
    private transient Logger logger;
    private AbstractApplicationContext beanContext;
    private Properties prop;
    private boolean forceLoad;
    private transient org.apache.hadoop.hbase.client.Connection connection;
    private transient HttpUtils httpUtils;


    public ObdHourDataPersistBolt(Properties prop, boolean forceLoad) {
        this.prop = prop;
        this.forceLoad = forceLoad;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf,context);
        prop = PropertiesUtil.initProp(prop, forceLoad);
        LogbackInitUtil.changeLogback(prop,true);
        beanContext = MyApplicationContext.getDefaultContext();
        logger = LoggerFactory.getLogger(ObdHourDataPersistBolt.class);
        connection = (org.apache.hadoop.hbase.client.Connection) beanContext.getBean(OtherKey.DataDealKey.HBASE_CONNECTION);
        httpUtils = (HttpUtils) beanContext.getBean(OtherKey.DataDealKey.HTTP_UTILS);

    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        try {
            String msg = tuple.getString(0);
            String msgId = tuple.getString(1);
            if (StringUtils.isBlank(msg))
                return;

            if(!StreamKey.ObdStream.OBD_HOUR_BOLT_S.equals(tuple.getSourceStreamId()))
                return;

            resultPersist(msg, StrategyChoose.generateStrategy(prop.getProperty(PropKey.DEAL_STRATEGY), httpUtils,
                    prop.getProperty("url_base"), HttpURIUtil.OBD_HOUR_SAVE,
                    connection, HBaseTable.HOUR_STATISTICS.getTableName(), HBaseTable.HOUR_STATISTICS.getFirstFamilyName(),
                    HbaseColumn.HourStatisticsCloumn.obdHourColumns,
                    ObdHourTransfor.class), StreamTypeDefine.OBD_TYPE, CstConstants.TIME_SELECT.HOUR);

            //持久化完成,发送到后端处理
            collector.emit(StreamKey.ObdStream.OBD_HOUR_PERSIST_S,new Values(msg,msgId, IntegrityHourCalcEnum.OBD.getValue()));
        } catch (Exception e) {
            logger.warn("处理小时obd持久化出错",e);
        }

    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(StreamKey.ObdStream.OBD_HOUR_PERSIST_S, new Fields(new String[] {
                StreamKey.ObdStream.OBD_KEY_F, StreamKey.ObdStream.OBD_KEY_S,StreamKey.ObdStream.OBD_KEY_T}));
    }
    @Override
    public void cleanup() {
        super.cleanup();
        if(beanContext!=null)
            beanContext.close();
    }


}
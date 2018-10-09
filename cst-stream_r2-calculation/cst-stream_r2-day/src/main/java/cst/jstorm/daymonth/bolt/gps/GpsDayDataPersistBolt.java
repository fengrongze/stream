package cst.jstorm.daymonth.bolt.gps;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.cst.stream.common.CstConstants;
import com.cst.stream.common.HBaseTable;
import com.cst.stream.common.HbaseColumn;
import com.cst.stream.common.StreamTypeDefine;
import com.cst.stream.stathour.gps.GpsDayTransfor;
import com.cst.jstorm.commons.stream.constants.OtherKey;
import com.cst.jstorm.commons.stream.constants.PropKey;
import com.cst.jstorm.commons.stream.constants.StreamKey;
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
 * create on 2018/5/9 17:11
 * @Description gps day persist
 * @title
 */
public class GpsDayDataPersistBolt extends BaseBasicBolt {
    private static final long serialVersionUID = -3294975428537135835L;
    private transient Logger logger;
    private AbstractApplicationContext beanContext;
    private Properties prop;
    private boolean forceLoad;
    private transient org.apache.hadoop.hbase.client.Connection connection;
    private transient HttpUtils httpUtils;


    public GpsDayDataPersistBolt(Properties prop, boolean forceLoad) {
        this.prop = prop;
        this.forceLoad = forceLoad;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf,context);
        prop = PropertiesUtil.initProp(prop, forceLoad);
        LogbackInitUtil.changeLogback(prop,true);
        if (beanContext == null) beanContext = MyApplicationContext.getDefaultContext();
        logger = LoggerFactory.getLogger(GpsDayDataPersistBolt.class);
        connection = (org.apache.hadoop.hbase.client.Connection) beanContext.getBean(OtherKey.DataDealKey.HBASE_CONNECTION);
        httpUtils= (HttpUtils) beanContext.getBean(OtherKey.DataDealKey.HTTP_UTILS);
    }
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String msg = tuple.getString(0);
        if (StringUtils.isBlank(msg))
            return;

        resultPersist(msg, StrategyChoose.generateStrategy(prop.getProperty(PropKey.DEAL_STRATEGY), httpUtils,
                prop.getProperty("url_base"), HttpURIUtil.GPS_DAY_SAVE,
                connection, HBaseTable.DAY_STATISTICS.getTableName(),
                HBaseTable.DAY_STATISTICS.getSecondFamilyName(),
                HbaseColumn.DayStatisticsCloumn.gpsDayColumns,
                GpsDayTransfor.class), StreamTypeDefine.GPS_TYPE, CstConstants.TIME_SELECT.DAY);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
    @Override
    public void cleanup() {
        super.cleanup();
        if(beanContext!=null)
            beanContext.close();
    }



}

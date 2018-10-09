package cst.jstorm.hour.bolt.trace;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.cst.stream.common.CstConstants;
import com.cst.stream.common.HBaseTable;
import com.cst.stream.common.HbaseColumn;
import com.cst.stream.common.StreamTypeDefine;
import com.cst.stream.stathour.trace.TraceHourTransfor;
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

import static com.cst.jstorm.commons.stream.operations.GeneralPersistUtils.firstDataPersist;


/**
 * @author Johnney.Chiu
 * create on 2018/5/9 17:10
 * @Description no delay persist
 * @title
 */
public class TraceHourFirstDataPBolt extends BaseBasicBolt {

    private static final long serialVersionUID = 2383776043617788246L;
    private transient Logger logger;
    private AbstractApplicationContext beanContext;
    private Properties prop;
    private boolean forceLoad;
    private transient org.apache.hadoop.hbase.client.Connection connection;
    private transient HttpUtils httpUtils;

    public TraceHourFirstDataPBolt(Properties prop, boolean forceLoad) {
        this.prop = prop;
        this.forceLoad = forceLoad;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf,context);
        prop = PropertiesUtil.initProp(prop, forceLoad);
        LogbackInitUtil.changeLogback(prop, true);
        if (beanContext == null) beanContext = MyApplicationContext.getDefaultContext();
        logger = LoggerFactory.getLogger(TraceHourFirstDataPBolt.class);
        connection = (org.apache.hadoop.hbase.client.Connection) beanContext.getBean(OtherKey.DataDealKey.HBASE_CONNECTION);
        httpUtils = (HttpUtils) beanContext.getBean(OtherKey.DataDealKey.HTTP_UTILS);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String msg = tuple.getString(0);
        if (StringUtils.isBlank(msg))
            return;

        if(!StreamKey.TraceStream.TRACE_HOUR_BOLT_FIRST_DATA.equals(tuple.getSourceStreamId()))
            return;
        firstDataPersist(msg, StrategyChoose.generateStrategy(prop.getProperty(PropKey.DEAL_STRATEGY),
                httpUtils, prop.getProperty("url_base"), HttpURIUtil.TRACE_HOUR_SOURCE_SAVE,
                connection, HBaseTable.HOUR_FIRST_ZONE.getTableName(),
                HBaseTable.HOUR_FIRST_ZONE.getFirstFamilyName(),
                HbaseColumn.HourSourceColumn.traceHourColumns,
                TraceHourTransfor.class
        ),StreamTypeDefine.TRACE_TYPE, CstConstants.TIME_SELECT.HOUR);
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

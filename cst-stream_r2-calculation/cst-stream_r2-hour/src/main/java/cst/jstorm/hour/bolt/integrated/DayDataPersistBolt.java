package cst.jstorm.hour.bolt.integrated;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.cst.stream.common.CstConstants;
import com.cst.stream.common.HBaseTable;
import com.cst.stream.common.HbaseColumn;
import com.cst.stream.stathour.am.AmDayTransfor;
import com.cst.stream.stathour.de.DeDayTransfor;
import com.cst.stream.stathour.trace.TraceDayTransfor;
import com.cst.stream.stathour.tracedelete.TraceDeleteDayTransfor;
import com.cst.stream.stathour.voltage.VoltageDayTransfor;
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

import static com.cst.jstorm.commons.stream.operations.GeneralPersistUtils.dataPersist;


/**
 * @author Johnney.Chiu
 * create on 2018/3/12 16:30
 * @Description day data persist
 * @title
 */
public class DayDataPersistBolt  extends BaseBasicBolt {
    private static final long serialVersionUID = 6012011461377995704L;
    private transient Logger logger;
    private AbstractApplicationContext beanContext;
    private Properties prop;
    private boolean forceLoad;
    private transient org.apache.hadoop.hbase.client.Connection connection;
    private transient HttpUtils httpUtils;


    public DayDataPersistBolt(Properties prop, boolean forceLoad) {
        this.prop = prop;
        this.forceLoad = forceLoad;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf,context);
        prop = PropertiesUtil.initProp(prop, forceLoad);
        LogbackInitUtil.changeLogback(prop,true);
        if (beanContext == null) beanContext = MyApplicationContext.getDefaultContext();
        logger = LoggerFactory.getLogger(DayDataPersistBolt.class);
        connection = (org.apache.hadoop.hbase.client.Connection) beanContext.getBean(OtherKey.DataDealKey.HBASE_CONNECTION);
        httpUtils= (HttpUtils) beanContext.getBean(OtherKey.DataDealKey.HTTP_UTILS);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String msg = tuple.getString(0);
        if(StringUtils.isBlank(msg))
            return;

        switch (tuple.getSourceStreamId()){
            case StreamKey.AmStream.AM_DAY_BOLT_S:
                dataPersist(msg, StrategyChoose.generateStrategy(prop.getProperty(PropKey.DEAL_STRATEGY), httpUtils,
                        prop.getProperty("url_base"), HttpURIUtil.AM_DAY_SAVE,
                        connection, HBaseTable.DAY_STATISTICS.getTableName(),
                        HBaseTable.DAY_STATISTICS.getFirstFamilyName(),
                        HbaseColumn.DayStatisticsCloumn.amDayColumns,
                        AmDayTransfor.class), CstConstants.TIME_SELECT.DAY);
                break;
            case StreamKey.DeStream.DE_DAY_BOLT_S:
                dataPersist(msg,StrategyChoose.generateStrategy(prop.getProperty(PropKey.DEAL_STRATEGY), httpUtils,
                        prop.getProperty("url_base"), HttpURIUtil.DE_DAY_SAVE,
                        connection, HBaseTable.DAY_STATISTICS.getTableName(),
                        HBaseTable.DAY_STATISTICS.getFirstFamilyName(),
                        HbaseColumn.DayStatisticsCloumn.deDayColumns,
                        DeDayTransfor.class), CstConstants.TIME_SELECT.DAY);
                break;


            case StreamKey.TraceStream.TRACE_DAY_BOLT_S:
                dataPersist(msg,StrategyChoose.generateStrategy(prop.getProperty(PropKey.DEAL_STRATEGY), httpUtils,
                        prop.getProperty("url_base"), HttpURIUtil.TRACE_DAY_SAVE,
                        connection, HBaseTable.DAY_STATISTICS.getTableName(),
                        HBaseTable.DAY_STATISTICS.getFirstFamilyName(),
                        HbaseColumn.DayStatisticsCloumn.traceDayColumns,
                        TraceDayTransfor.class), CstConstants.TIME_SELECT.DAY);
                break;
            case StreamKey.TraceDeleteStream.TRACE_DELETE_DAY_BOLT_S:
                dataPersist(msg,StrategyChoose.generateStrategy(prop.getProperty(PropKey.DEAL_STRATEGY), httpUtils,
                        prop.getProperty("url_base"), HttpURIUtil.TRACE_DELETE_DAY_SAVE,
                        connection, HBaseTable.DAY_STATISTICS.getTableName(),
                        HBaseTable.DAY_STATISTICS.getFirstFamilyName(),
                        HbaseColumn.DayStatisticsCloumn.traceDeleteDayColumns,
                        TraceDeleteDayTransfor.class), CstConstants.TIME_SELECT.DAY);
                break;
            case StreamKey.VoltageStream.VOLTAGE_DAY_BOLT_S:
                dataPersist(msg,StrategyChoose.generateStrategy(prop.getProperty(PropKey.DEAL_STRATEGY), httpUtils,
                        prop.getProperty("url_base"), HttpURIUtil.VOLTAGE_DAY_SAVE,
                        connection, HBaseTable.DAY_STATISTICS.getTableName(),
                        HBaseTable.DAY_STATISTICS.getFirstFamilyName(),
                        HbaseColumn.DayStatisticsCloumn.voltageDayColumns,
                        VoltageDayTransfor.class), CstConstants.TIME_SELECT.DAY);
                break;
        }
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

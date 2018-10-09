package cst.jstorm.daymonth.bolt.mileage;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.cst.jstorm.commons.stream.constants.OtherKey;
import com.cst.jstorm.commons.stream.constants.PropKey;
import com.cst.jstorm.commons.stream.operations.hbasestrategy.StrategyChoose;
import com.cst.jstorm.commons.utils.HttpURIUtil;
import com.cst.jstorm.commons.utils.LogbackInitUtil;
import com.cst.jstorm.commons.utils.PropertiesUtil;
import com.cst.jstorm.commons.utils.http.HttpUtils;
import com.cst.jstorm.commons.utils.spring.MyApplicationContext;
import com.cst.stream.common.CstConstants;
import com.cst.stream.common.HBaseTable;
import com.cst.stream.common.HbaseColumn;
import com.cst.stream.common.StreamTypeDefine;
import com.cst.stream.stathour.mileage.MileageDayTransfor;
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
 * @Description mileage day persist
 * @title
 */
public class MileageDayDataPersistBolt extends BaseBasicBolt {
    private static final long serialVersionUID = -5625949858712765241L;
    private transient Logger logger;
    private AbstractApplicationContext beanContext;
    private Properties prop;
    private boolean forceLoad;
    private transient org.apache.hadoop.hbase.client.Connection connection;
    private transient HttpUtils httpUtils;

    public MileageDayDataPersistBolt(Properties prop, boolean forceLoad) {
        this.prop = prop;
        this.forceLoad = forceLoad;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf,context);
        prop = PropertiesUtil.initProp(prop, forceLoad);
        LogbackInitUtil.changeLogback(prop,true);
        if (beanContext == null) beanContext = MyApplicationContext.getDefaultContext();
        logger = LoggerFactory.getLogger(MileageDayDataPersistBolt.class);
        connection = (org.apache.hadoop.hbase.client.Connection) beanContext.getBean(OtherKey.DataDealKey.HBASE_CONNECTION);
        httpUtils= (HttpUtils) beanContext.getBean(OtherKey.DataDealKey.HTTP_UTILS);
    }
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        try {
            String msg = tuple.getString(0);
            String msgId = tuple.getString(1);
            if (StringUtils.isBlank(msg))
                return;

            resultPersist(msg, StrategyChoose.generateStrategy(prop.getProperty(PropKey.DEAL_STRATEGY), httpUtils,
                    prop.getProperty("url_base"), HttpURIUtil.MILEAGE_DAY_SAVE,
                    connection, HBaseTable.DAY_STATISTICS.getTableName(),
                    HBaseTable.DAY_STATISTICS.getEighthFamilyName(),
                    HbaseColumn.DayStatisticsCloumn.mileageDayColumns,
                    MileageDayTransfor.class), StreamTypeDefine.MILEAGE_TYPE, CstConstants.TIME_SELECT.DAY);

            //持久化完成,发送到后端处理
            //collector.emit(StreamKey.ObdStream.OBD_DAY_PERSIST_S,new Values(msg,msgId, IntegrityDayCalcEnum.OBD.getValue()));
        } catch (Throwable e) {
            logger.warn("mileage data persist warn ",e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        /*declarer.declareStream(StreamKey.ObdStream.OBD_DAY_PERSIST_S, new Fields(new String[] {
                StreamKey.ObdStream.OBD_KEY_F, StreamKey.ObdStream.OBD_KEY_S,StreamKey.ObdStream.OBD_KEY_T}));
    */}
    @Override
    public void cleanup() {
        super.cleanup();
        if(beanContext!=null)
            beanContext.close();
    }




}

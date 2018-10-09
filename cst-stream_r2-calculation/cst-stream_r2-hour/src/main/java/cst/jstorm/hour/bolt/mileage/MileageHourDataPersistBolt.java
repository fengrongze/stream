package cst.jstorm.hour.bolt.mileage;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.cst.jstorm.commons.stream.constants.IntegrityHourCalcEnum;
import com.cst.jstorm.commons.stream.constants.OtherKey;
import com.cst.jstorm.commons.stream.constants.PropKey;
import com.cst.jstorm.commons.stream.constants.StreamKey;
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
import com.cst.stream.stathour.mileage.MileageHourTransfor;
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
public class MileageHourDataPersistBolt extends BaseBasicBolt {


    private static final long serialVersionUID = 4180883262628793100L;
    private transient Logger logger;
    private AbstractApplicationContext beanContext;
    private Properties prop;
    private boolean forceLoad;
    private transient org.apache.hadoop.hbase.client.Connection connection;
    private transient HttpUtils httpUtils;


    public MileageHourDataPersistBolt(Properties prop, boolean forceLoad) {
        this.prop = prop;
        this.forceLoad = forceLoad;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf,context);
        prop = PropertiesUtil.initProp(prop, forceLoad);
        LogbackInitUtil.changeLogback(prop,true);
        beanContext = MyApplicationContext.getDefaultContext();
        logger = LoggerFactory.getLogger(MileageHourDataPersistBolt.class);
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

            if(!StreamKey.MileageStream.MILEAGE_HOUR_BOLT_S.equals(tuple.getSourceStreamId()))
                return;

            resultPersist(msg, StrategyChoose.generateStrategy(prop.getProperty(PropKey.DEAL_STRATEGY), httpUtils,
                    prop.getProperty("url_base"), HttpURIUtil.MILEAGE_HOUR_SAVE,
                    connection, HBaseTable.HOUR_STATISTICS.getTableName(), HBaseTable.HOUR_STATISTICS.getEighthFamilyName(),
                    HbaseColumn.HourStatisticsCloumn.mileageHourColumns,
                    MileageHourTransfor.class), StreamTypeDefine.MILEAGE_TYPE, CstConstants.TIME_SELECT.HOUR);

        } catch (Exception e) {
            logger.warn("处理小时obd持久化出错",e);
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
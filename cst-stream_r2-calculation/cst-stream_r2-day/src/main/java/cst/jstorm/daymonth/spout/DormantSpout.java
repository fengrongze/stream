package cst.jstorm.daymonth.spout;
/**
 * @author Johnney.Chiu
 * create on 2018/8/10 12:00
 * @Description 休眠包
 * @title
 */

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import com.cst.jstorm.commons.stream.constants.KafkaTopicKey;
import com.cst.jstorm.commons.stream.constants.OtherKey;
import com.cst.jstorm.commons.stream.constants.StreamKey;
import com.cst.jstorm.commons.utils.KafkaConsumerUtil;
import com.cst.jstorm.commons.utils.LogbackInitUtil;
import com.cst.jstorm.commons.utils.PropertiesUtil;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.cst.jstorm.commons.stream.operations.DataParseFromSource.goDormancy;

public class DormantSpout extends BaseRichSpout {
    private static final Logger logger = LoggerFactory.getLogger(DormantSpout.class);
    private static final long serialVersionUID = -967344615108562969L;
    private Properties prop;
    private boolean forceLoad;
    private transient KafkaConsumer<String,byte[]> consumer;
    private SpoutOutputCollector collector;
    private Map<String, String> map;

    public DormantSpout(Properties prop, boolean forceLoad) {
        this.prop = prop;
        this.forceLoad = forceLoad;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        // TODO Auto-generated method stub
        this.collector = collector;
        PropertiesUtil.initProp(prop, forceLoad);
        LogbackInitUtil.changeLogback(prop,true);
        initConsumer();
        map = new HashMap<String,String>() {{
            put("dormancy_start", prop.getProperty("dormancy_start", "01:00:00"));
            put("dormancy_end", prop.getProperty("dormancy_end", "04:00:00"));
        }};
    }

    @Override
    public void nextTuple() {
// TODO Auto-generated method stub
        long start = System.currentTimeMillis();
        try {
            ConsumerRecords<String, byte[]> records = consumer.poll(NumberUtils.toInt(prop.getProperty("cst.stream.kafka.poll.interval", "1000")));
            for (ConsumerRecord<String, byte[]> record : records) {
                goDormancy(record.value(), collector,map);
            }
            logger.debug("dormancy topic deal time:{}", (System.currentTimeMillis() - start));
        }catch (Exception e){
            logger.error("dormancy day consumer data error",e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(StreamKey.DormancyStream.DORMANCY_DAY_BOLT_F, new Fields(new String[] {
                StreamKey.DormancyStream.DORMANCY_KEY_F, StreamKey.DormancyStream.DORMANCY_KEY_S}));

    }

    private void initConsumer() {
        this.consumer = KafkaConsumerUtil.buildKafkaConsumer(prop,OtherKey.GroupTail.DORMANCY_DAY);
        this.consumer.subscribe(Arrays.asList(prop.getProperty(KafkaTopicKey.DORMANCY_TOPIC)));
    }

    @Override
    public void activate() {
        super.deactivate();
        KafkaConsumerUtil.deActiveConsumer(consumer);
    }

    @Override
    public void fail(Object msgId) {
        super.fail(msgId);
        logger.error("ack failed execute data:{}",msgId);
    }
}

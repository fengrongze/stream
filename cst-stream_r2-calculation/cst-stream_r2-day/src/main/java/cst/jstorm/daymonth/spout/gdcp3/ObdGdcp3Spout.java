package cst.jstorm.daymonth.spout.gdcp3;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import com.cst.jstorm.commons.stream.constants.KafkaTopicKey;
import com.cst.jstorm.commons.stream.constants.OtherKey;
import com.cst.jstorm.commons.stream.constants.StreamKey;
import com.cst.jstorm.commons.stream.operations.Gdcp3DataParseFromSource;
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
import java.util.Map;
import java.util.Properties;

import static com.cst.jstorm.commons.stream.operations.DataParseFromSource.goOBD;

public class ObdGdcp3Spout extends BaseRichSpout {
    private static final Logger logger = LoggerFactory.getLogger(ObdGdcp3Spout.class);
    private static final long serialVersionUID = 7563570532367804995L;

    private Properties prop;
    private boolean forceLoad;
    private transient KafkaConsumer<String, String> consumer;
    private SpoutOutputCollector collector;

    public ObdGdcp3Spout(Properties prop, boolean forceLoad) {
        this.prop = prop;
        this.forceLoad = forceLoad;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        PropertiesUtil.initProp(prop, forceLoad);
        LogbackInitUtil.changeLogback(prop,true);
        initConsumer();
    }

    @Override
    public void nextTuple() {
        long start = System.currentTimeMillis();
        try {
            ConsumerRecords<String, String> records = consumer.poll(NumberUtils.toInt(prop.getProperty("cst.stream.kafka.poll.interval", "1000")));
            for (ConsumerRecord<String, String> record : records) {
                Gdcp3DataParseFromSource.goOBD(record.value(), collector);
            }
            logger.debug("gdcp3 obd topic deal time:{}", (System.currentTimeMillis() - start));
        }catch (Exception e){
            logger.error("gdcp3 obd day consumer data error",e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(StreamKey.ObdStream.OBD_GDCP3_HOUR_BOLT_F, new Fields(StreamKey.ObdStream.OBD_KEY_F,
                StreamKey.ObdStream.OBD_KEY_S));
    }



    private void initConsumer() {
        this.consumer = KafkaConsumerUtil.buildKafkaConsumer(prop, OtherKey.Gdcp3GroupTail.OBD_DAY);
        this.consumer.subscribe(Arrays.asList(prop.getProperty(KafkaTopicKey.Gdcp3TopicKey.GDCP3_OBD_TOPIC)));
    }
    @Override
    public void fail(Object msgId) {
        super.fail(msgId);
        logger.error("ack failed execute data:{}",msgId);
    }

    @Override
    public void deactivate() {
        KafkaConsumerUtil.deActiveConsumer(consumer);
        super.deactivate();
    }

}

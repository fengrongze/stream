package cst.jstorm.daymonth.spout;

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
import java.util.Map;
import java.util.Properties;

import static com.cst.jstorm.commons.stream.operations.DataParseFromSource.goMileageData;

public class MileageSpout extends BaseRichSpout {
    private static final Logger logger = LoggerFactory.getLogger(MileageSpout.class);
    private static final long serialVersionUID = 7563570532367804995L;

    private Properties prop;
    private boolean forceLoad;
    private transient KafkaConsumer<String, byte[]> consumer;
    private SpoutOutputCollector collector;

    public MileageSpout(Properties prop, boolean forceLoad) {
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
            ConsumerRecords<String, byte[]> records = consumer.poll(NumberUtils.toInt(prop.getProperty("cst.stream.kafka.poll.interval", "1000")));
            for (ConsumerRecord<String, byte[]> record : records) {
                goMileageData(record.value(), collector);
            }
            logger.debug("mileage topic deal time:{}", (System.currentTimeMillis() - start));
        }catch (Exception e){
            logger.error("mileage day consumer data error",e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(StreamKey.MileageStream.MILEAGE_HOUR_BOLT_F, new Fields(StreamKey.MileageStream.MILEAGE_KEY_F,
                StreamKey.MileageStream.MILEAGE_KEY_S));
    }



    private void initConsumer() {
        this.consumer = KafkaConsumerUtil.buildKafkaConsumer(prop, OtherKey.GroupTail.MILEAGE_DAY);
        this.consumer.subscribe(Arrays.asList(prop.getProperty(KafkaTopicKey.MILEAGE_TOPIC)));
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

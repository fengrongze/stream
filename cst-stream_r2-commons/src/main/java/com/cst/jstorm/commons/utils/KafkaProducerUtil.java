package com.cst.jstorm.commons.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaProducerUtil {
	private final static Logger log = LoggerFactory.getLogger(KafkaProducerUtil.class);

	public static <K,V> Producer<K,V> buildKafkaConsumer(Properties prop) {
		Properties props = new Properties();
		props.put("bootstrap.servers", prop.getProperty("streamkafka.bootstrap.servers"));
		props.put("acks", prop.getProperty("streamkafka.acks"));
		props.put("retries", prop.getProperty("streamkafka.retries"));
		props.put("batch.size", prop.getProperty("streamkafka.batch.size"));
		props.put("linger.ms", prop.getProperty("streamkafka.linger.ms"));
		props.put("buffer.memory", prop.getProperty("streamkafka.buffer.memory"));
		props.put("key.serializer", prop.getProperty("streamkafka.key.serializer"));
		props.put("value.serializer", prop.getProperty("streamkafka.value.serializer"));
		return new KafkaProducer(props);
	}

}

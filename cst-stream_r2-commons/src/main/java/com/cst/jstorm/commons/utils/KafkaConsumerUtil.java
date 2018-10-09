package com.cst.jstorm.commons.utils;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaConsumerUtil {
	private final static Logger log = LoggerFactory.getLogger(KafkaConsumerUtil.class);

	public static <K,V> KafkaConsumer<K,V> buildKafkaConsumer(Properties prop,String groupTail) {
		Properties props = new Properties();
		props.put("bootstrap.servers", prop.getProperty("cst.stream.kafka.bootstrap.servers"));
		props.put("group.id",prop.getProperty("cst.stream.kafka.group.id")+groupTail);
		props.put("request.timeout.ms", prop.getProperty("cst.stream.kafka.request.timeout.ms"));
		props.put("enable.auto.commit", Boolean.valueOf(prop.getProperty("cst.stream.kafka.enable.auto.commit")));
		props.put("auto.commit.interval.ms",prop.getProperty("cst.stream.kafka.auto.commit.interval.ms"));
		props.put("session.timeout.ms",prop.getProperty("cst.stream.kafka.session.timeout.ms"));
		props.put("auto.offset.reset",prop.getProperty("cst.stream.kafka.auto.offset.reset"));
		props.put("heartbeat.interval.ms",prop.getProperty("cst.stream.kafka.heartbeat.interval.ms"));
		props.put("max.partition.fetch.bytes",prop.getProperty("cst.stream.kafka.max.partition.fetch.bytes"));
		props.put("key.deserializer", prop.getProperty("cst.stream.kafka.key.deserializer"));
		props.put("value.deserializer", prop.getProperty("cst.stream.kafka.value.deserializer"));
		return new KafkaConsumer(props);
	}

	public static KafkaConsumer<String,String> buildStringKafkaConsumer(Properties prop,String groupTail) {
		Properties props = new Properties();
		props.put("bootstrap.servers", prop.getProperty("cst.stream.kafka.bootstrap.servers"));
		props.put("group.id",prop.getProperty("cst.stream.kafka.group.id")+groupTail);
		props.put("request.timeout.ms", prop.getProperty("cst.stream.kafka.request.timeout.ms"));
		props.put("enable.auto.commit", Boolean.valueOf(prop.getProperty("cst.stream.kafka.enable.auto.commit")));
		props.put("auto.commit.interval.ms",prop.getProperty("cst.stream.kafka.auto.commit.interval.ms"));
		props.put("session.timeout.ms",prop.getProperty("cst.stream.kafka.session.timeout.ms"));
		props.put("auto.offset.reset",prop.getProperty("cst.stream.kafka.auto.offset.reset"));
		props.put("heartbeat.interval.ms",prop.getProperty("cst.stream.kafka.heartbeat.interval.ms"));
		props.put("max.partition.fetch.bytes",prop.getProperty("cst.stream.kafka.max.partition.fetch.bytes"));
		props.put("key.deserializer", prop.getProperty("cst.stream.kafka.key.deserializer"));
		props.put("value.deserializer", prop.getProperty("cst.stream.kafka.string.value.deserializer"));
		return new KafkaConsumer(props);
	}

	public static <K,V> KafkaConsumer<K,V> buildSomeOtherKafkaConsumer(Properties prop,String groupTail) {
		Properties props = new Properties();
		props.put("bootstrap.servers", prop.getProperty("cst.stream.kafka.bootstrap.servers"));
		props.put("group.id",prop.getProperty("cst.stream.kafka.others.group.id")+groupTail);
		props.put("request.timeout.ms", prop.getProperty("cst.stream.kafka.request.timeout.ms"));
		props.put("enable.auto.commit", Boolean.valueOf(prop.getProperty("cst.stream.kafka.enable.auto.commit")));
		props.put("auto.commit.interval.ms",prop.getProperty("cst.stream.kafka.auto.commit.interval.ms"));
		props.put("session.timeout.ms",prop.getProperty("cst.stream.kafka.session.timeout.ms"));
		props.put("auto.offset.reset",prop.getProperty("cst.stream.kafka.auto.offset.reset"));
		props.put("heartbeat.interval.ms",prop.getProperty("cst.stream.kafka.heartbeat.interval.ms"));
		props.put("max.partition.fetch.bytes",prop.getProperty("cst.stream.kafka.max.partition.fetch.bytes"));
		props.put("key.deserializer", prop.getProperty("cst.stream.kafka.key.deserializer"));
		props.put("value.deserializer", prop.getProperty("cst.stream.kafka.value.deserializer"));
		return new KafkaConsumer(props);
	}
	public static KafkaConsumer<String,byte[]> buildKafkaConsumer(Map config) {
		Map<String, Object> map = new HashMap<>();
		map.put("bootstrap.servers", config.get("cst.stream.kafka.bootstrap.servers"));
		map.put("group.id",config.get("cst.stream.kafka.group.id"));
		map.put("request.timeout.ms", config.get("cst.stream.kafka.request.timeout.ms"));
		map.put("enable.auto.commit", Boolean.valueOf((String)config.get("cst.stream.kafka.enable.auto.commit")));
		map.put("auto.commit.interval.ms",config.get("cst.stream.kafka.auto.commit.interval.ms"));
		map.put("session.timeout.ms",config.get("cst.stream.kafka.session.timeout.ms"));
		map.put("auto.offset.reset",config.get("cst.stream.kafka.auto.offset.reset"));
		map.put("heartbeat.interval.ms",config.get("cst.stream.kafka.heartbeat.interval.ms"));
		map.put("max.partition.fetch.bytes",config.get("cst.stream.kafka.max.partition.fetch.bytes"));
		map.put("key.deserializer", config.get("cst.stream.kafka.key.deserializer"));
		map.put("value.deserializer", config.get("cst.stream.kafka.value.deserializer"));
		return new KafkaConsumer<String,byte[]>(map);
	}

	public static void deActiveConsumer(KafkaConsumer consumer){


	}
	
}

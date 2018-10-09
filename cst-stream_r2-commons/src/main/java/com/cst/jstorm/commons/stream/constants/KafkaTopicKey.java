package com.cst.jstorm.commons.stream.constants;

/**
 * @author Johnney.chiu
 * create on 2017/11/20 14:55
 * @Description 定义kafka订阅的topic
 */
public class KafkaTopicKey {

    public static final String OBD_TOPIC = "kafka.topic.obd";

    public static final String ELECTRIC_OBD_TOPIC = "kafka.topic.electric.obd";

    public static final String GPS_TOPIC = "kafka.topic.gps";

    public static final String AM_TOPIC = "kafka.topic.am";

    public static final String DE_TOPIC = "kafka.topic.de";

    public static final String TRACE_TOPIC = "kafka.topic.trace";

    public static final String VOLTAGE_TOPIC = "kafka.topic.voltage";

    public static final String TRACE_DELETE_TOPIC = "kafka.topic.trace.delete";

    public static final String DORMANCY_TOPIC = "kafka.topic.trace.dormancy";

    public static final String MILEAGE_TOPIC = "kafka.topic.mileage";

    public static final class Gdcp3TopicKey{

        public static final String GDCP3_OBD_TOPIC = "kafka.topic.obd.gdcp3";

        public static final String GDCP3_ELECTRIC_OBD_TOPIC = "kafka.topic.electric.obd.gdcp3";

        public static final String GDCP3_GPS_TOPIC = "kafka.topic.gps.gdcp3";

        public static final String GDCP3_DE_TOPIC = "kafka.topic.de.gdcp3";

        public static final String GDCP3_TRACE_TOPIC = "kafka.topic.trace.gdcp3";

        public static final String GDCP3_OTHER_TOPIC = "kafka.topic.other.gdcp3";

        public static final String GDCP3_TRACE_DELETE_TOPIC = "kafka.topic.trace.delete.gdcp3";

        public static final String GDCP3_DORMANCY_TOPIC = "kafka.topic.trace.dormancy.gdcp3";

    }



}

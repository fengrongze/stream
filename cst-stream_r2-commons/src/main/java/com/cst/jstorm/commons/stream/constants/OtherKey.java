package com.cst.jstorm.commons.stream.constants;

/**
 * @author Johnney.chiu
 * create on 2018/1/24 10:27
 * @Description other data key
 */
public class OtherKey {

    public static final long MIN_OBD_DATA_TIME = 1388505600000l;

    public static final class MIDLLE_DEAL{
        //persist
        public static final String PERSIST_KEY = "persist";

        //nextBolt
        public static final String NEXT_KEY = "next";

        public static final String NO_DELAY_TIME_ZONE = "no_delay_time_zone";

        public static final String FIRST_TIME_ZONE = "first_time_zone";



        public static final String RESULT_BOOLEAN = "result_bool";

        public static final String INTERVAL = "interval";

        public static final String FMT = "fmt";

        public static final String TIME_FORMAT = "time_format";


        public static final String REDIS_HEAD = "type_head";

        public static final String NEXT_STREAM = "next_stream";

        public static final String LAST_VALUE_EXPIRE_TIME = "lastValueExpireTime";

        public static final String ZONE_VALUE_EXPIRE_TIME = "zoneValueExpireTime";

        public static final String LAST_DATA_PARAM = "lastKeyType";

        public static final String BESINESS_KEY_TYPE = "keyType";

        public static final String ZONE_SCHEDULE_EXPIRE_TIME = "zoneScheduleExpireTime";

    }

    public static final class DataDealKey{
        public static final String HBASE_CONNECTION = "hBaseConnection";

        public static final String HTTP_UTILS = "httpUtils";

        public static final String TABLE_NAME = "tableName";

        public static final String FAMILY_NAME = "familyName";

        public static final String DEAL_CLASS = "class";

        public static final String TABLE_COLUMNS = "columns";

        public static final String TIME_SELECT = "timeSelect";

        public static final String BASE_URL = "baseUrl";

        public static final String DETAIL_URL = "detailUrl";

        public static final String ROW_KEY = "rowKey";

        public static final String QUERY_URL = "queryUrl";

        public static final String SAVE_URL = "saveUrl";

        public static final String DEAL_STRATEGY_HTTP = "http_client";

        public static final String DEAL_STRATEGY_HBASE = "hbase_client";

        public static final String CAR_ID = "carId";

        public static final String TIME = "time";

        public static final String PARAMS = "params";

        public static final String ROWKEY_GENERATE = "rowKeyGenerate";

    }
    public static final class GroupTail{
        public static final String OTHER_HOUR = "_hour";
        public static final String GPS_HOUR = "_gps_hour";
        public static final String OBD_HOUR = "_obd_hour";
        public static final String ELECTRIC_OBD_HOUR = "_electric_obd_hour";
        public static final String VOLTAGE_HOUR = "_voltage_hour";
        public static final String AM_HOUR = "_am_hour";
        public static final String DE_HOUR = "_de_hour";
        public static final String TRACE_HOUR = "_trace_hour";
        public static final String TRACE_DELETE_HOUR = "_trace_delete_hour";
        public static final String MILEAGE_HOUR = "_mileage_hour";

        public static final String OTHER_DAY = "_day";
        public static final String GPS_DAY = "_gps_day";
        public static final String OBD_DAY = "_obd_day";
        public static final String ELECTRIC_OBD_DAY = "_electric_obd_day";
        public static final String VOLTAGE_DAY = "_voltage_day";
        public static final String AM_DAY = "_am_day";
        public static final String DE_DAY = "_de_day";
        public static final String TRACE_DAY = "_trace_day";
        public static final String TRACE_DELETE_DAY = "_trace_delete_day";
        public static final String MILEAGE_DAY = "_mileage_day";
        public static final String DORMANCY_DAY = "_dormancy_day";


    }

    public static final class Gdcp3GroupTail{
        public static final String OTHER_HOUR = "_gdcp3_hour";
        public static final String GPS_HOUR = "_gdcp3_gps_hour";
        public static final String OBD_HOUR = "_gdcp3_obd_hour";
        public static final String ELECTRIC_OBD_HOUR = "_gdcp3_electric_obd_hour";
        public static final String VOLTAGE_HOUR = "_gdcp3_voltage_hour";
        public static final String AM_HOUR = "_gdcp3_am_hour";
        public static final String DE_HOUR = "_gdcp3_de_hour";
        public static final String TRACE_HOUR = "_gdcp3_trace_hour";
        public static final String TRACE_DELETE_HOUR = "_gdcp3_trace_delete_hour";

        public static final String OTHER_DAY = "_gdcp3_day";
        public static final String GPS_DAY = "_gdcp3_gps_day";
        public static final String OBD_DAY = "_gdcp3_obd_day";
        public static final String ELECTRIC_OBD_DAY = "_gdcp3_electric_obd_day";
        public static final String VOLTAGE_DAY = "_gdcp3_voltage_day";
        public static final String AM_DAY = "_gdcp3_am_day";
        public static final String DE_DAY = "_gdcp3_de_day";
        public static final String TRACE_DAY = "_gdcp3_trace_day";
        public static final String TRACE_DELETE_DAY = "_gdcp3_trace_delete_day";
        public static final String DORMANCY_DAY = "_gdcp3_dormancy_day";



    }


}

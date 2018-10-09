package com.cst.stream.common;

/**
 * @author Johnney.Chiu
 * create on 2018/6/8 11:49
 * @Description stream 缓存信息
 * @title
 */
public class StreamRedisConstants {

    public static final class StreamRedisFormat{

        /**
         * 小时第一条数据
         * f 代表小时的第一条标志
         * 第一个 s 数据类型标志 type {@link HourKey } {@link DayKey}{@link MonthKey } {@link YearKey }
         * 第二个 s 代表车id
         * 第三个 s 代表时间标志
         */
        public static final String FIRST_ZONE_FMT = "f_%s_%s_%s";



        /**
         * 计算结果数据
         * r 代表结果标志的第一条标志
         * 第一个 s 数据类型标志 type {@link HourKey } {@link DayKey}{@link MonthKey } {@link YearKey }
         * 第二个 s 代表车id
         * 第三个 s 代表时间标志
         */
        public static final String RESULT_ZONE_FMT = "r_%s_%s_%s";


        /**
         * 计算小时中间结果数据
         * l 代表最近计算结果标志
         * 第一个 s 代表数据类型标志 type {@link HourKey } {@link DayKey}
         * 第二个 s 代表车id
         */
        public static final String LATEST_ZONE_FMT = "l_%s_%s";


        /**
         * 计算小时中间结果数据
         * l 代表最近计算结果标志
         * 第一个 s 代表数据类型标志 type {@link MonthKey } {@link YearKey }
         * 第二个 s 代表车id
         */
        public static final String LATEST_ZONE_TIME_FMT = "l_%s_%s_%s";

        /**
         * 完整性数据数据key
         * i 代表完整性标志
         * 第一个 s 数据类型标志
         * 第二个 s 代表车id
         * 第三个 s 代表时间标志
         */
        public static final String INTEGRITY_FMT = "i_%s_%s_%s";

        /**
         * 完整性数据数据数据项目key
         * ii 代表完整性标志,1+2+4+8....
         * 第一个 s 数据类型标志
         * 第二个 s 代表车id
         * 第三个 s 代表时间标志
         */
        public static final String INTEGRITY_ITEM_FMT = "ii_%s_%s_%s";

        /**
         * 获取在缓存中第一条记录
         * @param type {@link HourKey } {@link DayKey} {@link MonthKey } {@link YearKey }
         * @param carId
         * @param timeZone
         * @return
         */
        public static final String getFirstZoneRedisKey(String type,String carId,String timeZone){
            return String.format(FIRST_ZONE_FMT, type, carId, timeZone);
        }

        /**
         * 获取在缓存中存的计算结果
         * @param type type {@link HourKey } {@link DayKey}{@link MonthKey } {@link YearKey }
         * @param carId
         * @param timeZone
         * @return
         */
        public static final String getResultZoneRedisKey(String type,String carId,String timeZone){
            return String.format(RESULT_ZONE_FMT, type, carId, timeZone);
        }

        /**
         * 获取缓存中最近一条数据
         * @param type type {@link HourKey } {@link DayKey}
         * @param carId
         * @return
         */
        public static final String getLatestZoneRedisKey(String type,String carId){
            return String.format(LATEST_ZONE_FMT, type, carId);
        }

        /**
         * 获取缓存中最近一条数据
         * @param type type {@link MonthKey } {@link YearKey }
         * @param carId
         * @return
         */
        public static final String getLatestZoneTimeRedisKey(String type,String carId,String timeStr){
            return String.format(LATEST_ZONE_TIME_FMT, type, carId,timeStr);
        }

        /**
         * 获取完整数据key
         * @param type
         * @param carId
         * @param timeZone
         * @return
         */
        public static final String getIntegrityDataRedisKey(String type,String carId,String timeZone){
            return String.format(INTEGRITY_FMT, type, carId, timeZone);
        }

        /**
         * 获取完整数据数据项key
         * @param type
         * @param carId
         * @param timeZone
         * @return
         */
        public static final String getIntegrityItemRedisKey(String type,String carId,String timeZone){
            return String.format(INTEGRITY_ITEM_FMT, type, carId, timeZone);
        }
    }


    public static final class HourKey {
        /**
         * GPS
         */
        public static final String HOUR_GPS = "h_gps";
        /**
         * OBD
         */
        public static final String HOUR_OBD = "h_obd";
        /**
         * 告警
         */
        public static final String HOUR_AM = "h_am";
        /**
         * 轨迹
         */
        public static final String HOUR_TRACE = "h_tc";
        /**
         * 轨迹delete
         */
        public static final String HOUR_TRACE_DELETE = "h_td";

        /**
         * 电瓶电压
         */
        public static final String HOUR_VOLTAGE = "h_vlt";

        /**
         * OTHER
         */
        public static final String HOUR_OTHER = "h_oth";
        /**
         * STATUS
         */
        public static final String HOUR_STATUS = "h_sts";
        /**
         * de
         */
        public static final String HOUR_DE = "h_de";

        /**
         * mileage
         */
        public static final String HOUR_MILEAGE = "h_mil";
    }

    public static final class DayKey {
        /**
         * GPS
         */
        public static final String DAY_GPS = "d_gps_o";
        /**
         * OBD
         */
        public static final String DAY_OBD = "d_obd_o";
        /**
         * 告警
         */
        public static final String DAY_AM = "d_am_o";
        /**
         * 轨迹
         */
        public static final String DAY_TRACE = "d_tc_o";
        /**
         * 轨迹delete
         */
        public static final String DAY_TRACE_DELETE = "d_td_o";

        /**
         * 电瓶电压
         */
        public static final String DAY_VOLTAGE = "d_vlt_o";

        /**
         * OTHER
         */
        public static final String DAY_OTHER = "d_oth_o";
        /**
         * STATUS
         */
        public static final String DAY_STATUS = "d_sts_o";
        /**
         * de
         */
        public static final String DAY_DE = "d_de_o";

        /**
         * mileage
         */
        public static final String DAY_MILEAGE = "d_mil_o";
    }

    public static final class MonthKey {
        /**
         * GPS
         */
        public static final String MONTH_GPS = "m_gps";
        /**
         * OBD
         */
        public static final String MONTH_OBD = "m_obd";
        /**
         * 告警
         */
        public static final String MONTH_AM = "m_am";
        /**
         * 轨迹
         */
        public static final String MONTH_TRACE = "m_tc";
        /**
         * 轨迹delete
         */
        public static final String MONTH_TRACE_DELETE = "m_td";

        /**
         * 电瓶电压
         */
        public static final String MONTH_VOLTAGE = "m_vlt";

        /**
         * OTHER
         */
        public static final String MONTH_OTHER = "m_oth";
        /**
         * STATUS
         */
        public static final String MONTH_STATUS = "m_sts";
        /**
         * de
         */
        public static final String MONTH_DE = "m_de";
        /**
         * mileage
         */
        public static final String MONTH_MILEAGE = "m_mil";
    }
    public static final class YearKey {
        /**
         * GPS
         */
        public static final String YEAR_GPS = "y_gps";
        /**
         * OBD
         */
        public static final String YEAR_OBD = "y_obd";
        /**
         * 告警
         */
        public static final String YEAR_AM = "y_am";
        /**
         * 轨迹
         */
        public static final String YEAR_TRACE = "y_tc";
        /**
         * 轨迹delete
         */
        public static final String YEAR_TRACE_DELETE = "y_td";

        /**
         * 电瓶电压
         */
        public static final String YEAR_VOLTAGE = "y_vlt";

        /**
         * OTHER
         */
        public static final String YEAR_OTHER = "y_oth";
        /**
         * STATUS
         */
        public static final String YEAR_STATUS = "y_sts";
        /**
         * de
         */
        public static final String YEAR_DE = "y_de";
        /**
         * mileage
         */
        public static final String YEAR_MILEAGE = "y_mil";
    }


    public static class ExpireTime{

        public static final int GAS_PRICE_TIME = 24*60*60;

        public static final int GAS_NUM_TIME = 2*24*60*60;

        public static final int ZONE_VALUE_EXPIRE_TIME = 2 * 60 * 60;

        public static final int LAST_CALC_VALUE_EXPIRE_TIME =  24 * 60 * 60;


        /**
         * 小时结果数据暂存时间
         */
        public static final int HOUR_RESULT_DATA_EXPIRE_TIME = 2 * 60 * 60;

        /**
         * 天结果数据暂存时间
         */
        public static final int DAY_RESULT_DATA_EXPIRE_TIME = 2 * 24 * 60 * 60;

        /**
         * 月结果数据暂存时间
         */
        public static final int MONTH_RESULT_DATA_EXPIRE_TIME = 5 * 24 * 60 * 60;

        /**
         * 第一条小时数据暂存时间
         */
        public static final int FIRST_DATA_EXPIRE_TIME=2 * 60 * 60;

    }


    public static class GasKey {
        /**
         * 车用的油标
         * s是车的id
         */
        public static final String GAS_NUM_FMT = "GN_%s";


        /**
         * 车用的油标
         * 第一个s是时间
         * 第二个s是城市
         * 第三个s是油的标号
         */
        public static final String GAS_PRICE_FMT = "FU_%s_%s_%s";


        /**
         *s 是carid
         */
        public static final String CAR_CITY_FMT = "CC_%s";


        /**
         * s是车的id
         * @param carId
         * @return
         */
        public static final String getGasNumKey(String carId){
            return String.format(GAS_NUM_FMT, carId);
        }
        /**
         *
         * @param carId
         * @return
         */
        public static final String getCarCityKey(String carId){
            return String.format(CAR_CITY_FMT, carId);
        }


        /**
         * 返回油标
         * @param timeStr
         * @param city
         * @param gasNum
         * @return
         */
        public static final String getGasPriceKey(String timeStr,String city,String gasNum){
            return String.format(GAS_PRICE_FMT,timeStr,city,gasNum);
        }




        public static final String NIGHTY_GAS = "90";
        public static final String NIGHTY_THREE_GAS = "93";
        public static final String NIGHTY_SEVEN_GAS = "97";
        public static final String ZERO_GAS = "0";



    }

    public static final class IntegrityKey {
        /**
         * day
         */
        public static final String DAY_INTEGRITY = "day_integrity";

        /**
         * hour
         */
        public static final String HOUR_INTEGRITY = "hour_integrity";
    }

        public static final class ZoneScheduleKey {

        private static final String CAR_KEY_SET = "ncs_%s";

        public static String getCarKeySetRedisKey(String timeZone){
            return String.format(CAR_KEY_SET, timeZone);
        }
    }

    public static final class DispatchRecord{
        private static final String RECORD_KEY = "drk_%s_%s_%s";

        public static String getDispatchRecordKey(String type,String carid,String time){
            return String.format(RECORD_KEY, type,carid,time);
        }
    }

}

package com.cst.jstorm.commons.stream.constants;

public class RedisKey {

	public static final String STORM_REDISCLUSTER="storm.rediscluster";


	public static class HourKey {
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
		public static final String HOUR_TRACE = "h_trace";
		/**
		 * 轨迹delete
		 */
		public static final String HOUR_TRACE_DELETE = "h_trace_delete";

		/**
		 * 电瓶电压
		 */
		public static final String HOUR_VOLTAGE = "h_voltage";

		/**
		 * OTHER
		 */
		public static final String HOUR_OTHER = "h_other";
		/**
		 * STATUS
		 */
		public static final String HOUR_STATUS = "h_status";
		/**
		 * de
		 */
		public static final String HOUR_DE = "h_de";
	}
	public static class LastHourKey {
		/**
		 * GPS
		 */
		public static final String HOUR_GPS = "lh_gps";
		/**
		 * OBD
		 */
		public static final String HOUR_OBD = "lh_obd";
		/**
		 * 告警
		 */
		public static final String HOUR_AM = "lh_am";
		/**
		 * 轨迹
		 */
		public static final String HOUR_TRACE = "lh_trace";
		/**
		 * 轨迹 delete
		 */
		public static final String HOUR_TRACE_DELETE = "lh_trace_delete";
		/**
		 * 电瓶电压
		 */
		public static final String HOUR_VOLTAGE = "lh_voltage";
		/**
		 * OTHER
		 */
		public static final String HOUR_OTHER = "lh_other";
		/**
		 * STATUS
		 */
		public static final String HOUR_STATUS = "lh_status";
		/**
		 * de
		 */
		public static final String HOUR_DE = "lh_de";
	}

	public static class DayKey {
		/**
		 * GPS
		 */
		public static final String DAY_GPS = "d_gps";
		/**
		 * OBD
		 */
		public static final String DAY_OBD = "d_obd";
		/**
		 * 告警
		 */
		public static final String DAY_AM = "d_am";
		/**
		 * 轨迹
		 */
		public static final String DAY_TRACE = "d_trace";
		/**
		 * 轨迹删除
		 */
		public static final String DAY_TRACE_DELETE = "d_trace_delete";
		/**
		 * 删除
		 */
		public static final String DAY_VOLTAGE = "d_voltage";
		/**
		 * OTHER
		 */
		public static final String DAY_OTHER = "d_other";
		/**
		 * STATUS
		 */
		public static final String DAY_STATUS = "d_status";
		/**
		 * de
		 */
		public static final String DAY_DE = "d_de";
	}
	public static class LastDayKey {
		/**
		 * GPS
		 */
		public static final String DAY_GPS = "ld_gps";
		/**
		 * OBD
		 */
		public static final String DAY_OBD = "ld_obd";
		/**
		 * 告警
		 */
		public static final String DAY_AM = "ld_am";
		/**
		 * 轨迹
		 */
		public static final String DAY_TRACE = "ld_trace";
		/**
		 * 轨迹删除
		 */
		public static final String DAY_TRACE_DELETE = "ld_trace_delete";
		/**
		 * 电瓶电压
		 */
		public static final String DAY_VOLTAGE = "ld_voltage";
		/**
		 * OTHER
		 */
		public static final String DAY_OTHER = "ld_other";
		/**
		 * STATUS
		 */
		public static final String DAY_STATUS = "ld_status";
		/**
		 * de
		 */
		public static final String DAY_DE = "ld_de";
	}

	public static class GPS_REDIS_HOUR {
		//最大搜星数
		public static final String MAX_SATELLITE_NUM = "maxSatelliteNum";

		//gps上报数
		public static final String GPS_COUNT = "gpsCount";

		public static final String DIN = "din";

		public static final String DIN_CHANGE = "dinChange";

		public static final String TIME = "time";

		public static final String IS_NON_LOCAL = "isnonlocal";
	}
	public static class DE_REDIS_HOUR{
		//急加速
		public static final String RAPID_ACCELERATION_COUNT = "RAC";
		//急减速
		public static final String RAPID_DECELERATION_COUNT = "RDC";
		//急转弯
		public static final String SHARP_TURN_COUNT = "STC";

	}

	public static final String KEY_CARCITIES_HASH = "carCities";

	public static final String KEY_GAS_PRICE = "fu:";

	public static final String KEY_GAS_NUM = "gasnum";

	public static final String KEY_SEP = "_";

	public static final String RESULT_FLAG = "R_";


	public static final String FIRST_FLAG = "F_";

	public static class ExpireTime{

		public static final int GAS_PRICE_TIME = 24*60*60;

		public static final int GAS_NUM_TIME = 2*24*60*60;

		public static final int ZONE_VALUE_EXPIRE_TIME = 2 * 60 * 60;

		public static final int MONTH_ZONE_VALUE_EXPIRE_TIME = 31 * 24 * 60 * 60;

		public static final int YEAR_ZONE_VALUE_EXPIRE_TIME = 365 * 24 * 60 * 60;

		public static final int LAST_CALC_VALUE_EXPIRE_TIME =  24 * 60 * 60;

		public static final int ZONE_SCHEDULE_DEALT_EXPIRE_TIME = 24 * 60 * 60;

		public static final int DISPATCH_HOUR_RECORD_EXPIRE_TIME = 24 * 60 * 60;

		public static final int DISPATCH_DAY_RECORD_EXPIRE_TIME = 7 * 24 * 60 * 60;

	}


}

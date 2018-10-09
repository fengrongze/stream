package com.cst.jstorm.commons.stream.constants;

public class RedisField {
	
	//GPS
	/**
	 * 最大搜星数
	 */
	public static final String SATELLITE_MAX = "satellite_max";
	/**
	 * 车是否在外地（0-否，1-是）
	 */
	public static final String IS_OUT = "is_out";
	/**
	 * GPS上报次数
	 */
	public static final String GPS_TIMES = "gps_times";
	/**
	 * 是否更换车机（0-否，1-是）
	 */
	public static final String IS_CHANGE_DEVICE = "is_change_device";
	
	//OBD
	/**
	 * 最大速度
	 */
	public static final String SPEED_MAX = "speed_max";
	/**
	 * 是否上了高速（0-否，1-是）
	 */
	public static final String IS_HIGHSPEED = "is_highspeed";
	/**
	 * 是否夜间开车（0-否，1-是）
	 */
	public static final String IS_NIGHT_DRIVE = "is_night_drive";
	/**
	 * 是否开车（0-否，1-是）
	 */
	public static final String IS_DRIVE = "is_drive";
	/**
	 * 里程
	 */
	public static final String MILEAGE = "mileage";
	/**
	 * 油耗
	 */
	public static final String FUEL = "fuel";
	/**
	 * 行驶时间
	 */
	public static final String RUNTIME = "runtime";
	
	//AM
	/**
	 * 急加速次数
	 */
	public static final String ACC_TIMES = "acc_times";
	/**
	 * 急减速次数
	 */
	public static final String DEC_TIMES = "dec_times";
	/**
	 * 急转弯次数
	 */
	public static final String TURN_TIMES = "turn_times";
	/**
	 * 点火次数
	 */
	public static final String IGNITE_TIMES = "ignite_times";
	/**
	 * 熄火次数
	 */
	public static final String FLAMEOUT_TIMES = "flameout_times";
	/**
	 * 插入次数
	 */
	public static final String INSERT_TIMES = "insert_times";
	/**
	 * 碰撞次数
	 */
	public static final String CRASH_TIMES = "crash_times";
	/**
	 * 超速次数
	 */
	public static final String SPEED_TIMES = "speed_times";
	/**
	 * 是否失联（0-否，1-是）
	 */
	public static final String IS_MISSING = "is_missing";
	/**
	 * 拔出时长
	 */
	public static final String PULL_DURATION = "pull_duration";
	
	//TRACE
	/**
	 * 单次最大行驶时间
	 */
	public static final String SINGLE_DRIVE_DURATION_MAX = "single_drive_duration_max";
	
	//OTHER
	/**
	 * 最高电瓶电压
	 */
	public static final String VOLTAGE_MAX = "voltage_max";
	/**
	 * 最低电瓶电压
	 */
	public static final String VOLTAGE_MIN = "voltage_min";
	
	//STATUS
	/**
	 * 与平台交互次数
	 */
	public static final String PLATFORM_TIMES = "platform_times";
	/**
	 * 连接次数
	 */
	public static final String CONNECT_TIMES = "connect_times";
	
	/**
	 * 是否疲劳驾驶（0-否，1-是）
	 */
	public static final String IS_FATIGUE = "is_fatigue";
	
	/**
	 * 小时
	 */
	private static final String HOUR = "hour";
	/**
	 * 天
	 */
	private static final String DAY = "day";
	
	/**
	 * 获取小时field
	 * @param field
	 * @return
	 */
	public static String getHourField(String field) {
		return field + "_" + HOUR;
	}
	
	/**
	 * 获取天field
	 * @param field
	 * @return
	 */
	public static String getDayField(String field) {
		return field + "_" + DAY;
	}
}

package com.cst.stream.common;

/**
 * @author Johnney.chiu
 * create on 2017/11/28 11:03
 * @Description 报警类型
 */
public class AlarmTypeConst {

    /**
     * 汽车点火上报
     */
    public static final int ALARM_CAR_IGNITION = 0x0001;
    /**
     * 汽车熄火上报
     */
    public static final int ALARM_CAR_FLAMEOUT = 0x0002;
    /**
     * 汽车设防上报
     */
    public static final int ALARM_GARRISON = 0x0003;
    /**
     * 汽车撤防上报
     */
    public static final int ALARM_WITHDRAW_GARRISON = 0x0004;
    /**
     * 汽车低电压报警
     */
    public static final int ALARM_LOW_VOLTAGE = 0x0005;
    /**
     * 汽车怠速过长上报
     */
    public static final int ALARM_LOW_IDLING = 0x0006;
    public static final int ALARM_LOW_IDLING_CLEAR = -ALARM_LOW_IDLING;
    /**
     * 汽车车门非法打开
     */
    public static final int ALARM_DOOR_ILLEGAL_OPEN = 0x0007;
    /**
     * 碰撞报警
     */
    public static final int ALARM_COLLISION = 0x0008;
    /**
     * 拖车报警
     */
    public static final int ALARM_TOWED_AWAY = 0x0009;
    /**
     * 翻车报警
     */
    public static final int ALARM_TURN_OVER = 0x000a;
    /**
     * 定位过长报警
     */
    public static final int ALARM_LONG_POSITIONING_TIME = 0x000b;
    /**
     * 汽车非法点火报警
     */
    public static final int ALARM_ILL_IGNITION = 0x0013;
    /**
     * 故障码报警
     */
    public static final int ALARM_FAULT_CODE = 0x0014;

    // linx 2014-05-21 update
    /**
     * 水温报警
     */
    public static final int ALARM_WATER_TEMPERATURE = 0x010B;
    public static final int ALARM_WATER_TEMPERATURE_CLEAR = -ALARM_WATER_TEMPERATURE;
    /**
     * 超速报警
     */
    public static final int ALARM_OVERSPEED = 0x010C;
    public static final int ALARM_OVERSPEED_CLEAR = -ALARM_OVERSPEED;
    /**
     * RTC硬件故障报警
     */
    public static final int ALARM_RTC_HARDWARE_FAULT = 0x0205;

    // linx 2014-06-19 update
    /**
     * 高速空档滑行报警
     */
    public static final int AlARM_TOPSPEED_NEUTRAL_SLIDE_PARA = 0x010D;
    /**
     * 疲劳驾驶报警
     */
    public static final int ALARM_FATIGUE_DRIVING_PARA = 0x010E;
    public static final int ALARM_FATIGUE_DRIVING_PARA_CLEAR = -ALARM_FATIGUE_DRIVING_PARA;
    /**
     * 低水温高转速报警
     */
    public static final int ALARM_LOW_WARM_HIGH_SPEED_PARA = 0x010F;
    public static final int ALARM_LOW_WARM_HIGH_SPEED_PARA_CLEAR = -ALARM_LOW_WARM_HIGH_SPEED_PARA;
    /**
     * 油耗不支持报警
     */
    public static final int ALARM_OIL_CONSUMPTION_NOT_SUPPORT = 0x011A;

    /**
     * OBD不支持报警
     */
    public static final int ALARM_OBD_NOT_SUPPORT = 0x011B;

    /**
     * 锁车失败提醒
     */
    public static final int LOCK_FAILURE = 0x0302;
    /**
     * 超时未设防提醒
     */
    public static final int OUTTIME_NO_FORTIFY = 0x0303;
    /**
     * 设防玻璃未关提醒
     */
    public static final int FORTIFY_NO_CLOSE_WINDOW = 0X0301;

    // 车机类故障
    /**
     * 终端拔出报警
     */
    public static final int ALARM_TERMINAL_PULL = 0x000c;
    /**
     * 终端插入报警
     */
    public static final int ALARM_TERMINAL_INSERT = 0x000d;
    /**
     * GPS 模块故障报警
     */
    public static final int ALARM_GPS_FAULT = 0x000e;
    /**
     * FLASH 故障报警
     */
    public static final int ALARM_FLASH_FAULT = 0x000f;
    /**
     * CAN 模块故障报警
     */
    public static final int ALARM_CAN_FAULT = 0x0010;
    /**
     * 3D 传感器故障报警
     */
    public static final int ALARM_3D_SENSOR_FAULT = 0x0011;
    /**
     * TTS 模块故障报警
     */
    public static final int ALARM_TTS_FAULT = 0x0012;

    // 车机状态告警 linx 2014-08-17
    /**
     * 车机失联报警
     */
    public static final int ALARM_DISAPPEAR_STATE = 0x0401;
    /**
     * 车机复联报警
     */
    public static final int ALARM_RECOVER_CONNECT_STATE = 0x0402;
    /**
     * 车机疑似插入报警
     */
    public static final int ALARM_PLUG_STATE = 0x0403;
    /**
     * 车机疑似拔出报警
     */
    public static final int ALARM_PUL_LOUT_STATE = 0x0404;

    /**
     * 系统故障告警
     */
    public static final int ALARM_CLEAR = 20001;

    /**
     * 车机疑似拔出插入其它车报警"OBD/安防";
     */
    public static final int ALARM_WRONG_INSERT_STATE = 0xA901;
    /**
     * 车机疑似拔出报警"GPS";
     */
    public static final int ALARM_LONG_NOGPSINF_TIME = 0xA902;
    /**
     * 安防告警  合并门、灯 ；以后可能会有锁、窗
     */
    public static final int ALARM_CAR_SECURITY=0xF300;
    /**
     * 熄火后未设防告警
     */
    public static final int ALARM_FLAMEOUT_GARRISON=0xF301;

    /**
     * 维修模式
     */
    public static final int SET_MAINTAIN_MODE=0xB001;
    public static final int QUIT_MAINTAIN_MODE=-SET_MAINTAIN_MODE;

    /**
     * 终端主电断电报警
     */
    public static final int ALARM_POWER_CUT_OFF=0x0501;
    /**
     * GPS天线开路报警
     */
    public static final int ALARM_GPS_OPEN_CIRCUIT=0x0502;
    /**
     * GPS天线短路报警
     */
    public static final int ALARM_GPS_SHORT_CIRCUIT=0x0503;
    /**
     * 车机未固定报警
     */
    public static final int ALARM_NOT_FIXED=0x0504;
    /**
     * OBD通信异常中断
     */
    public static final int ALARM_OBD_POWER_OFF=0x0042;
}

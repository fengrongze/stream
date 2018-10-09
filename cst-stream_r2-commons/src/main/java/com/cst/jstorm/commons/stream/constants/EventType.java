package com.cst.jstorm.commons.stream.constants;

/**
 * @author Johnney.Chiu
 * create on 2018/8/13 19:00
 * @Description 事件类别定义
 * @title
 */
public class EventType {

    /**
     * 点火
     */
    public static final int ALARM_CAR_IGNITION = 0xD010;

    /**
     * 熄火
     */
    public static final int ALARM_CAR_FLAMEOUT = 0xD011;

    /**
     *插入
     */
    public static final int ALARM_CAR_INSERT = 0xD001;


    /**
     * 疲劳驾驶
     */
    public static final int ALARM_FATIGUE_DRIVING = 0xD028;

    /**
     * 碰撞告警
     */
    public static final int ALARM_COLLISION = 0xE001;

    /**
     * 超速
     */
    public static final int ALARM_OVERSPEED = 0xD027;

    /**
     * 失联告警
     */
    public static final int ALARM_DISAPPEAR_STATE = 0xE006;


    /**
     * 拔出告警
     */
    public static final int ALARM_TERMINAL_PULL = 0xD002;


    /**
     * 急加速
     */
    public static final int EVENT_RAPID_ACCELERATION = 0xD020;

    /**
     * 急减速
     */
    public static final int EVENT_RAPID_DECELERATION = 0xD021;

    /**
     * 急转弯
     */
    public static final int EVENT_SHARP_TURN = 0xD022;


}

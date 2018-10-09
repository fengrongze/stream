package com.cst.stream.common;

/**
 * @author Johnney.chiu
 * create on 2018/1/24 17:29
 * @Description 公共变量
 */
public class CstConstants {
    /** Kafka数据有效时间 */
    public static final long DATA_VALID_SECONDS = 365 * 24 * 60 * 60;

    public enum TIME_SELECT{
        HOUR,DAY, MONTH,YEAR;
    }
}

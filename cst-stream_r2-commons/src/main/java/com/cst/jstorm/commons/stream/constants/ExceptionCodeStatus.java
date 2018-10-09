package com.cst.jstorm.commons.stream.constants;

/**
 * @author Johnney.Chiu
 * create on 2018/7/11 16:42
 * @Description
 * @title
 */
public enum ExceptionCodeStatus {
    CALC_NO_TIME(0,"计算但是不更新缓存中的时间"),CALC_DATA(1,"计算,更新所有的值"),CALC_RETURN(2,"不计算直接返回"),CALC_JUMP(3,"跳变状态");

    ExceptionCodeStatus(int code,String desc) {
        this.code = code;
        this.desc = desc;
    }
    private int code;
    private String desc;
}

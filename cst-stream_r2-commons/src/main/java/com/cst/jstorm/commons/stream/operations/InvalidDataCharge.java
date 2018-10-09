package com.cst.jstorm.commons.stream.operations;

import lombok.extern.slf4j.Slf4j;

/**
 * @author Johnney.Chiu
 * create on 2018/5/30 15:08
 * @Description
 * @title
 */
@Slf4j
public class InvalidDataCharge {
    public static final long MAX_VALID_SECONDS = 365 * 24 * 60 * 60;

    public static final long FUTURE_VALID_SECONDS = 2 * 24 * 60 * 60;

    public static final long LESS_VALID_SECONDS = 90 * 24 * 60 * 60;

    public static final  boolean invalidUpTime(String carId, String type, long time){

        long now = System.currentTimeMillis();
        if(time>now &&(time-now)>FUTURE_VALID_SECONDS * 1000){
            log.warn("Invalid data update time more than now:type={} carId={}, obdTime={}",type, carId, time);
            return true;
        }

        if (Math.abs(now - time) > MAX_VALID_SECONDS * 1000) {
            log.warn("Invalid data time:type={} carId={}, obdTime={}",type, carId, time);
            return true;
        }
        return false;
    }


    public static final  boolean invalidTooLessThanTime(long uploadTime){

        long now = System.currentTimeMillis();
        if (Math.abs(now - uploadTime) > LESS_VALID_SECONDS * 1000) {
            return true;
        }
        return false;
    }

}

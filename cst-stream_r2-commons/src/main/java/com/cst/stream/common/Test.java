package com.cst.stream.common;

import com.cst.stream.common.JsonHelper;
import com.cst.stream.stathour.obd.ObdYearTransfor;
import com.fasterxml.jackson.core.type.TypeReference;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

/**
 * @author Johnney.Chiu
 * create on 2018/9/10 20:41
 * @Description
 * @title
 */
public class Test {
    public static void main(String... args){
        String scheduleStart="16:00:00";
        String scheduleEnd = "16:30:00";

        Date date = new Date();
        LocalDateTime localDateTimeStart= DateTimeUtils.getSpecialOfDateBymillSeconds(date, scheduleStart, DateTimeUtils.TimeFormat.SHORT_TIME_PATTERN_NONE_WITHOUT_MIS);
        LocalDateTime localDateTimeEnd=DateTimeUtils.getSpecialOfDateBymillSeconds(date, scheduleEnd, DateTimeUtils.TimeFormat.SHORT_TIME_PATTERN_NONE_WITHOUT_MIS);
        LocalDateTime now = LocalDateTime.now();
        long l1=localDateTimeStart.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        long l2=localDateTimeEnd.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        long l3=now.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        /*System.out.println(l1>l2);
        System.out.println(l2>l3);*/
        if (now.isBefore(localDateTimeStart) || now.isAfter(localDateTimeEnd)) {
            System.out.println(11);
            return;
        }

        System.out.println("not");

    }
}

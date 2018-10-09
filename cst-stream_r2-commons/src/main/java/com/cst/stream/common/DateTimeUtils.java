package com.cst.stream.common;

import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static com.cst.stream.common.CstConstants.TIME_SELECT.YEAR;

/**
 * @author Johnney.Chiu
 * create on 2018/4/24 18:14
 * @Description
 * @title
 */
public class DateTimeUtils {


    // 默认时间格式
    private static final DateTimeFormatter DEFAULT_DATETIME_FORMATTER = TimeFormat.SHORT_DATE_PATTERN_LINE.formatter;
    private static final String YEAR_STR_FORMAT = "%s-01-01 00:00:00";
    private static final String MONTH_STR_FORMAT = "%s-01 00:00:00";
    private static final String DAY_STR_FORMAT = "%s 00:00:00";
    private static final String HOUR_STR_FORMAT = "%s:00:00";

    private DateTimeUtils() {};
    /**
     * String 转化为 LocalDate
     *
     * @param timeStr
     *            被转化的字符串
     * @return LocalDate
     */
    public static LocalDate parseDate(String timeStr) {
       return  LocalDate.parse(timeStr, DEFAULT_DATETIME_FORMATTER);

    }
    /**
     * String 转化为 LocalDate
     *
     * @param timeStr
     *            被转化的字符串
     * @return LocalDate
     */
    public static LocalDate parseDate(String timeStr, TimeFormat timeFormat) {
        return  LocalDate.parse(timeStr, timeFormat.formatter);

    }


    /**
     * String 转化为 LocalDateTime
     *
     * @param timeStr
     *            被转化的字符串
     * @param timeFormat
     *            转化的时间格式
     * @return LocalDateTime
     */
    public static LocalDateTime parseTime(String timeStr, TimeFormat timeFormat) {
        return LocalDateTime.parse(timeStr, timeFormat.formatter);

    }

    /**
     * LocalDateTime 转化为String
     *
     * @param time
     *            LocalDateTime
     * @return String
     */
    public static String parseTime(LocalDateTime time) {
        return DEFAULT_DATETIME_FORMATTER.format(time);

    }


    //获取指定日期的毫秒
    public static Long getMilliByTime(LocalDateTime time) {
        return time.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

    //获取指定日期的毫秒
    public static Long getMilliByDate(LocalDate date) {
        LocalDateTime localDateTime = LocalDateTime.of(date,LocalTime.of(0,0,0));

        return localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

    /**
     * LocalDateTime 时间转 String
     *
     * @param time
     *            LocalDateTime
     * @param format
     *            时间格式
     * @return String
     */
    public static String parseTime(LocalDateTime time, TimeFormat format) {
        return format.formatter.format(time);

    }

    /**
     * 获取当前时间
     *
     * @return
     */
    public static String getCurrentDateTime() {
        return DEFAULT_DATETIME_FORMATTER.format(LocalDateTime.now());
    }

    /**
     * 获取当前时间
     *
     * @param timeFormat
     *            时间格式
     * @return
     */
    public static String getCurrentDateTime(TimeFormat timeFormat) {
        return timeFormat.formatter.format(LocalDateTime.now());
    }
    public static String geLocalDateTimeStr(LocalDateTime localDateTime,TimeFormat timeFormat) {
        return timeFormat.formatter.format(localDateTime);
    }


    public static Date convertDateFromStr(String str,TimeFormat timeFormat){
        LocalDateTime ldt = LocalDateTime.parse(str,timeFormat.formatter);
        return convertLDTToDate(ldt);

    }

    public static Date convertLDTToDate(LocalDateTime time) {
        return Date.from(time.atZone(ZoneId.systemDefault()).toInstant());
    }

    public static String parseTime(Date date,TimeFormat timeFormat){
        return timeFormat.simpleDateFormat.format(date);
    }

    public static LocalDateTime convertDateoLDTT(Date date) {
        return LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
    }

    public static LocalDateTime getFirstOfDateBymillSeconds(Date date){
        return getSpecialOfDateBymillSeconds(date, "00:00:00.000");
    }
    public static LocalDateTime getLastOfDateBymillSeconds(Date date){
        return getSpecialOfDateBymillSeconds(date, "23:59:59.999");

    }

    public static LocalDateTime getSpecialOfDateBymillSeconds(Date date,String time){
        LocalDate ld = parseDate(TimeFormat.SHORT_DATE_PATTERN_LINE.simpleDateFormat.format(date));
        LocalTime lt = LocalTime.parse(time, TimeFormat.SHORT_TIME_PATTERN_NONE.formatter);
        return LocalDateTime.of(ld, lt);
    }

    public static long getAddTime(long timeStamp,CstConstants.TIME_SELECT timeSelect) {
        LocalDateTime localDateTime=convertDateoLDTT(new Date(timeStamp));
        LocalDateTime localDateTimeResult = LocalDateTime.now();
        switch (timeSelect) {
            case YEAR:
                localDateTimeResult=localDateTime.plusYears(1);
                break;
            case DAY:
                localDateTimeResult=localDateTime.plusDays(1);
                break;
            case MONTH:
                localDateTimeResult = localDateTime.plusMonths(1);
                break;
            case HOUR:
                localDateTimeResult = localDateTime.plusHours(1);
                break;
        }
        return getTimestampOfDateTime(localDateTimeResult);
    }
    public static long getMinusTime(long timeStamp,CstConstants.TIME_SELECT timeSelect) {
        LocalDateTime localDateTime=convertDateoLDTT(new Date(timeStamp));
        LocalDateTime localDateTimeResult = LocalDateTime.now();
        switch (timeSelect) {
            case YEAR:
                localDateTimeResult=localDateTime.minusYears(1);
                break;
            case DAY:
                localDateTimeResult=localDateTime.minusDays(1);
                break;
            case MONTH:
                localDateTimeResult = localDateTime.minusMonths(1);
                break;
            case HOUR:
                localDateTimeResult = localDateTime.minusHours(1);
                break;
        }
        return getTimestampOfDateTime(localDateTimeResult);
    }

    public static LocalDateTime getDateTimeOfTimestamp(long timestamp) {
        Instant instant = Instant.ofEpochMilli(timestamp);
        ZoneId zone = ZoneId.systemDefault();
        return LocalDateTime.ofInstant(instant, zone);
    }

    public static long getTimestampOfDateTime(LocalDateTime localDateTime) {
        ZoneId zone = ZoneId.systemDefault();
        Instant instant = localDateTime.atZone(zone).toInstant();
        return instant.toEpochMilli();
    }
    public static LocalDateTime getSpecialOfDateBymillSeconds(Date date,String time,TimeFormat timeFormat){
        LocalDate ld = parseDate(TimeFormat.SHORT_DATE_PATTERN_LINE.simpleDateFormat.format(date));
        LocalTime lt = LocalTime.parse(time, timeFormat.formatter);
        return LocalDateTime.of(ld, lt);
    }

    public static List<String> getBetweenDateStr(long lessTime,long moreTime,CstConstants.TIME_SELECT timeSelect){
        ZoneId zone = ZoneId.systemDefault();
        List<String> dateStrs = new LinkedList<>();
        if(lessTime>=moreTime)
            return dateStrs;
        Instant instantLess = Instant.ofEpochMilli(lessTime);
        Instant instantMore = Instant.ofEpochMilli(moreTime);
        LocalDateTime localDateTimeLess=LocalDateTime.ofInstant(instantLess, zone);
        LocalDateTime localDateTimeMore=LocalDateTime.ofInstant(instantMore, zone);


        String time;
        boolean flag=false;
        String yearMore=geLocalDateTimeStr(localDateTimeMore, TimeFormat.SHORT_YEAR_PATTERN);
        String monthMore=geLocalDateTimeStr(localDateTimeMore, TimeFormat.SHORT_MONTH_PATTERN);
        String dayMore=geLocalDateTimeStr(localDateTimeMore, TimeFormat.SHORT_DATE_PATTERN_LINE);
        String hourMore=geLocalDateTimeStr(localDateTimeMore, TimeFormat.SHORT_DATE_HOUR_PATTERN_LINE);
        while (true) {
            switch (timeSelect){
                case YEAR:{
                    localDateTimeLess=localDateTimeLess.plusYears(1);
                    time=geLocalDateTimeStr(localDateTimeLess, TimeFormat.SHORT_YEAR_PATTERN);
                    if (time.equals(yearMore)||localDateTimeLess.isAfter(localDateTimeMore)) {
                        flag = true;
                    }else{
                        time=String.format(YEAR_STR_FORMAT, time);
                    }

                }
                break;
                case MONTH:{
                    localDateTimeLess=localDateTimeLess.plusMonths(1);
                    time=geLocalDateTimeStr(localDateTimeLess, TimeFormat.SHORT_MONTH_PATTERN);
                    if (time.equals(monthMore)||localDateTimeLess.isAfter(localDateTimeMore)) {
                        flag = true;
                    }else{
                        time=String.format(MONTH_STR_FORMAT, time);
                    }
                }
                break;
                case DAY:{
                    localDateTimeLess=localDateTimeLess.plusDays(1);
                    time=geLocalDateTimeStr(localDateTimeLess, TimeFormat.SHORT_DATE_PATTERN_LINE);
                    if (time.equals(dayMore)||localDateTimeLess.isAfter(localDateTimeMore)) {
                        flag = true;
                    }else{
                        time=String.format(DAY_STR_FORMAT, time);
                    }
                }
                break;
                case HOUR:{
                    localDateTimeLess=localDateTimeLess.plusHours(1);
                    time=geLocalDateTimeStr(localDateTimeLess, TimeFormat.SHORT_DATE_HOUR_PATTERN_LINE);
                    if (time.equals(hourMore)||localDateTimeLess.isAfter(localDateTimeMore)) {
                        flag = true;
                    }else{
                        time=String.format(HOUR_STR_FORMAT, time);
                    }
                }
                break;
                default:
                {
                    localDateTimeLess=localDateTimeLess.plusHours(1);
                    time=geLocalDateTimeStr(localDateTimeLess, TimeFormat.SHORT_DATE_HOUR_PATTERN_LINE);
                    if (time.equals(hourMore)||localDateTimeLess.isAfter(localDateTimeMore)) {
                        flag = true;
                    }else{
                        time=String.format(HOUR_STR_FORMAT, time);
                    }
                }
            }
            if(flag)
                break;
            dateStrs.add(time);

        }
        return dateStrs;
    }

    public static List<Long> getBetweenTimestamp(long lessTime,long moreTime,CstConstants.TIME_SELECT timeSelect){
        ZoneId zone = ZoneId.systemDefault();
        List<Long> list=new ArrayList<>();
        if(lessTime>=moreTime)
            return list;

        Instant instantLess = Instant.ofEpochMilli(lessTime);
        Instant instantMore = Instant.ofEpochMilli(moreTime);
        LocalDateTime localDateTimeLess=LocalDateTime.ofInstant(instantLess, zone);
        LocalDateTime localDateTimeMore=LocalDateTime.ofInstant(instantMore, zone);


        long time;
        boolean flag=false;

        while (true) {
            switch (timeSelect){
                case YEAR:{
                    localDateTimeLess=localDateTimeLess.plusYears(1);
                    time=getTimestampOfDateTime(localDateTimeLess);
                    if (localDateTimeLess.isAfter(localDateTimeMore)) {
                        flag = true;
                    }

                }
                break;
                case MONTH:{
                    localDateTimeLess=localDateTimeLess.plusMonths(1);
                    time=getTimestampOfDateTime(localDateTimeLess);
                    if (localDateTimeLess.isAfter(localDateTimeMore)) {
                        flag = true;
                    }
                }
                break;
                case DAY:{
                    localDateTimeLess=localDateTimeLess.plusDays(1);
                    time=getTimestampOfDateTime(localDateTimeLess);
                    if (localDateTimeLess.isAfter(localDateTimeMore)) {
                        flag = true;
                    }
                }
                break;
                case HOUR:{
                    localDateTimeLess=localDateTimeLess.plusHours(1);
                    time=getTimestampOfDateTime(localDateTimeLess);
                    if (localDateTimeLess.isAfter(localDateTimeMore)) {
                        flag = true;
                    }
                }
                break;
                default:
                {
                    localDateTimeLess=localDateTimeLess.plusHours(1);
                    time=getTimestampOfDateTime(localDateTimeLess);
                    if (localDateTimeLess.isAfter(localDateTimeMore)) {
                        flag = true;
                    }
                }
            }
            if(flag)
                break;
            list.add(time);

        }
        return list;
    }


    public enum TimeFormat {

        /**
         * 时间格式 yyyy
         */
        SHORT_YEAR_PATTERN("yyyy"),

        /**
         * 时间格式 yyyy-MM
         */
        SHORT_MONTH_PATTERN("yyyy-MM"),

        //短时间格式 年月日
        /**
         *时间格式：yyyy-MM-dd
         */
        SHORT_DATE_PATTERN_LINE("yyyy-MM-dd"),

        /**
         *时间格式：yyyy-MM-dd HH
         */
        SHORT_DATE_HOUR_PATTERN_LINE("yyyy-MM-dd HH"),
        /**
         *时间格式：yyyy/MM/dd
         */
        SHORT_DATE_PATTERN_SLASH("yyyy/MM/dd"),
        /**
         *时间格式：yyyy\\MM\\dd
         */
        SHORT_DATE_PATTERN_DOUBLE_SLASH("yyyy\\MM\\dd"),
        /**
         *时间格式：yyyyMMdd
         */
        SHORT_DATE_PATTERN_NONE("yyyyMMdd"),

        /**
         *时间格式：HH:mm:ss.SSS
         */
        SHORT_TIME_PATTERN_NONE("HH:mm:ss.SSS"),

        /**
         *时间格式：HH:mm:ss
         */
        SHORT_TIME_PATTERN_NONE_WITHOUT_MIS("HH:mm:ss"),

        // 长时间格式 年月日时分秒
        /**
         *时间格式：yyyy-MM-dd HH:mm:ss
         */
        LONG_DATE_PATTERN_LINE("yyyy-MM-dd HH:mm:ss"),

        /**
         *时间格式：yyyy/MM/dd HH:mm:ss
         */
        LONG_DATE_PATTERN_SLASH("yyyy/MM/dd HH:mm:ss"),
        /**
         *时间格式：yyyy\\MM\\dd HH:mm:ss
         */
        LONG_DATE_PATTERN_DOUBLE_SLASH("yyyy\\MM\\dd HH:mm:ss"),
        /**
         *时间格式：yyyyMMdd HH:mm:ss
         */
        LONG_DATE_PATTERN_NONE("yyyyMMdd HH:mm:ss"),


        // 长时间格式 年月日时分秒 带毫秒
        LONG_DATE_PATTERN_WITH_MILSEC_LINE("yyyy-MM-dd HH:mm:ss.SSS"),

        LONG_DATE_PATTERN_WITH_MILSEC_SLASH("yyyy/MM/dd HH:mm:ss.SSS"),

        LONG_DATE_PATTERN_WITH_MILSEC_DOUBLE_SLASH("yyyy\\MM\\dd HH:mm:ss.SSS"),

        LONG_DATE_PATTERN_WITH_MILSEC_NONE("yyyyMMdd HH:mm:ss.SSS");


        private transient DateTimeFormatter formatter;
        private transient SimpleDateFormat simpleDateFormat;


        TimeFormat(String pattern) {
            formatter = DateTimeFormatter.ofPattern(pattern);
            simpleDateFormat = new SimpleDateFormat(pattern);
        }
    }

    public static void main(String... args){
        //获取当前时间
        System.out.println(DateTimeUtils.getCurrentDateTime());

        System.out.println(DateTimeUtils.getCurrentDateTime(TimeFormat.LONG_DATE_PATTERN_LINE));

        System.out.println(DateTimeUtils.getMilliByTime(LocalDateTime.now()));

        System.out.println(DateTimeUtils.geLocalDateTimeStr(DateTimeUtils.getFirstOfDateBymillSeconds(new Date()), TimeFormat.LONG_DATE_PATTERN_WITH_MILSEC_LINE));
        System.out.println(DateTimeUtils.geLocalDateTimeStr(DateTimeUtils.getLastOfDateBymillSeconds(new Date()), TimeFormat.LONG_DATE_PATTERN_WITH_MILSEC_LINE));

        LocalDateTime ldt = DateTimeUtils.getFirstOfDateBymillSeconds(new Date());

        Arrays.asList(1,2,3,6,7,8,10,3,2,1,100).stream().forEach(
                i->System.out.println(DateTimeUtils.geLocalDateTimeStr(ldt.plusDays(i), TimeFormat.LONG_DATE_PATTERN_WITH_MILSEC_LINE)));

        System.out.println(DateTimeUtils.getMilliByTime(DateTimeUtils.getFirstOfDateBymillSeconds(new Date())));
        System.out.println(DateTimeUtils.getMilliByTime(DateTimeUtils.getLastOfDateBymillSeconds(new Date())));

        String test = "2018-06-21 10:00:00";
        System.out.println(test);
        Date fromDate= convertDateFromStr(test, DateTimeUtils.TimeFormat.LONG_DATE_PATTERN_LINE);
        for (int i=0;i<20;i++) {
            fromDate = convertLDTToDate(convertDateoLDTT(fromDate).plusHours(1));
            System.out.println(parseTime(fromDate, TimeFormat.LONG_DATE_PATTERN_LINE));
        }

        System.out.println("#######################HOUR");
        List<String> list = DateTimeUtils.getBetweenDateStr(1531813888000L, 1531831888000L, CstConstants.TIME_SELECT.HOUR);
        list.stream().forEach(System.out::println);
        System.out.println("#######################DAY");
        list= DateTimeUtils.getBetweenDateStr(1531554688000L, 1531831888000L, CstConstants.TIME_SELECT.DAY);
        list.stream().forEach(System.out::println);
        System.out.println("#######################MONTH");
        list= DateTimeUtils.getBetweenDateStr(1531811802000L, 1531811802001L, CstConstants.TIME_SELECT.MONTH);
        list.stream().forEach(System.out::println);
        System.out.println("#######################YEAR");
        list= DateTimeUtils.getBetweenDateStr(1436860288000L, 1531831888000L, YEAR);
        list.stream().forEach(System.out::println);

        System.out.println(DateTimeUtils.getMilliByTime(parseTime("2018-07-17 19", TimeFormat.SHORT_DATE_HOUR_PATTERN_LINE)));
        System.out.println(DateTimeUtils.getMilliByDate(parseDate("2018-01-01", TimeFormat.SHORT_DATE_PATTERN_LINE)));
        System.out.println(DateTimeUtils.getMilliByTime(parseTime("2016-01-01 00:00:00", TimeFormat.LONG_DATE_PATTERN_LINE)));

        System.out.println(getSpecialOfDateBymillSeconds(new Date(),"01:00:00", TimeFormat.SHORT_TIME_PATTERN_NONE_WITHOUT_MIS));

        System.out.println("#######################DAY");
        List<Long> longs = DateTimeUtils.getBetweenTimestamp(1531554688000L, 1531831888000L, CstConstants.TIME_SELECT.DAY);
        longs.stream().forEach(System.out::println);
        System.out.println("#######################DAY2");
        longs = DateTimeUtils.getBetweenTimestamp(1531554688000L, 1531554688000L, CstConstants.TIME_SELECT.DAY);
        longs.stream().forEach(System.out::println);

    }

}



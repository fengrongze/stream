package com.cst.stream.common;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author Johnney.chiu
 * create on 2017/11/24 16:11
 * @Description 日期时间工具类
 */
public class DateTimeUtil {

    public final static String DEFAULT_DATE_HOUR = "yyyyMMddHH";
    public final static String DEFAULT_DATE_DAY = "yyyyMMdd";
    public final static String DEFAULT_DATE_MONTH = "yyyyMM";
    public final static String DEFAULT_DATE_YEAR = "yyyy";


    public final static String DEFAULT_DATE_DEFULT = "yyyy-MM-dd HH:mm:ss";
    public final static String DEFAULT_SHORT_DATE_DEFULT = "yyyy-MM-dd";

    public final static long ONE_HOUR = 60 * 60 * 1000;
    public final static long ONE_DAY = 24 * 60 * 60 * 1000;
    public final static long ONE_MONTH = 31 * 24 * 60 * 60 * 1000;
    public final static long ONE_YEAR = 12 * 31 * 24 * 60 * 60 * 1000;








    public static String toLongTimeString(Date dt,String fmt) {
        SimpleDateFormat myFmt = new SimpleDateFormat(fmt);
        return myFmt.format(dt);
    }

    public static String toLongTimeString(long timestamp,String fmt) {
        SimpleDateFormat myFmt = new SimpleDateFormat(fmt);
        Timestamp ts = new Timestamp(timestamp);
        return myFmt.format(ts);

    }

    public static Long strToTimestamp(String str,String fmt) throws ParseException {
        SimpleDateFormat myFmt = new SimpleDateFormat(fmt);
        Date date=myFmt.parse(str);
        return date.getTime();
    }

    /**
     * 获取单位秒数
     * @param timestamp
     * @return
     * @throws ParseException
     */
    public static Long getHourBase(long timestamp) throws ParseException {
        return strToTimestamp(DateTimeUtil.toLongTimeString(timestamp, DEFAULT_DATE_HOUR),DEFAULT_DATE_HOUR);
    }

    public static Long getDayBase(long timestamp) throws ParseException {
        return strToTimestamp(DateTimeUtil.toLongTimeString(timestamp, DEFAULT_DATE_DAY),DEFAULT_DATE_DAY);
    }

    /**
     * 当前月的第一天
     * @param timestamp
     * @return
     */
    public static long getMonthBase(long timestamp) throws ParseException {
        /*Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date(timestamp));
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        return calendar.getTimeInMillis();*/
        return strToTimestamp(DateTimeUtil.toLongTimeString(timestamp, DEFAULT_DATE_MONTH),DEFAULT_DATE_MONTH);
    }

    /**
     * 当前月的第一天
     * @param timestamp
     * @return
     */
    public static long getYearBase(long timestamp) throws ParseException {
        return strToTimestamp(DateTimeUtil.toLongTimeString(timestamp, DEFAULT_DATE_YEAR),DEFAULT_DATE_YEAR);
    }


    public static String getNow(String format){
        return toLongTimeString(new Date(), format);
    }



    public static List<String> calcLongTimeBetween(long time1, long time2, String fmt, long interval) throws ParseException {
        long timestamp1=strToTimestamp(DateTimeUtil.toLongTimeString(time1, fmt), fmt);
        long timestamp2=strToTimestamp(DateTimeUtil.toLongTimeString(time2, fmt), fmt);
        List<String> list = new ArrayList<>();
        long temp;
        if(timestamp1<timestamp2){
            temp = timestamp1+ interval;
            while (temp<timestamp2) {
                list.add(new SimpleDateFormat(fmt).format(new Timestamp(temp)));
                temp +=interval;
            }
        }else{
            temp=timestamp2+ interval;
            while (temp<timestamp1) {
                list.add(new SimpleDateFormat(fmt).format(new Timestamp(temp)));
                temp +=interval;
            }
        }
        return list;
    }

    public static boolean dateTimeDifferentBetween(long timestamp1, long timestamp2,String fmt) {
        return toLongTimeString(timestamp1,fmt).equals(toLongTimeString(timestamp2,fmt));
    }


    public static void main(String... args) throws ParseException {
        //System.out.println(DateTimeUtil.toLongTimeString(new Date()));
      /*  long cur = System.currentTimeMillis();
        System.out.println(DateTimeUtil.toLongTimeString(cur,DateTimeUtil.DEFAULT_DATE_HOUR));
        long cur2 = cur - 2 * ONE_HOUR;
        System.out.println(DateTimeUtil.toLongTimeString(cur2,DateTimeUtil.DEFAULT_DATE_HOUR));
        List<String> list = calcLongTimeBetween(cur,cur2,DateTimeUtil.DEFAULT_DATE_HOUR,DateTimeUtil.ONE_HOUR);
        for (String str: list) {
            System.out.println("haha:"+str);
        }
*/
       /* System.out.println(dateTimeDifferentBetween(1522134931000l,1522204210000l,DateTimeUtil.DEFAULT_DATE_DAY));
        for(int i=0;i<12;i++) {
            System.out.println("h_obd_6a8cc3ab01bf485a95c3181d1d5be02a_"+DateTimeUtil.toLongTimeString(System.currentTimeMillis()-(i*60*60*1000), DateTimeUtil.DEFAULT_DATE_HOUR));
        }
        System.out.println("d_obd_6a8cc3ab01bf485a95c3181d1d5be02a_"+DateTimeUtil.toLongTimeString(System.currentTimeMillis(), DateTimeUtil.DEFAULT_DATE_DAY));
*/
        //System.out.println(DateTimeUtil.strToTimestamp(DateTimeUtil.toLongTimeString(cur,DateTimeUtil.DEFAULT_DATE_HOUR),DateTimeUtil.DEFAULT_DATE_HOUR));
        //System.out.println(DateTimeUtil.toLongTimeString(DateTimeUtil.strToTimestamp(DateTimeUtil.toLongTimeString(cur,DateTimeUtil.DEFAULT_DATE_HOUR),DateTimeUtil.DEFAULT_DATE_HOUR),DateTimeUtil.DEFAULT_DATE_HOUR));

        System.out.println(DateTimeUtil.toLongTimeString(1517500800000l,DEFAULT_DATE_HOUR));
        System.out.println(DateTimeUtil.toLongTimeString(1517558400000l,DEFAULT_DATE_HOUR));

        System.out.println(getMonthBase(1517500800000l));
        System.out.println(getMonthBase(1517558400000l));



        List<String> strs= DateTimeUtil.calcLongTimeBetween(1530663370000L, 1530670750000L, DateTimeUtil.DEFAULT_DATE_HOUR, DateTimeUtil.ONE_HOUR);
        strs.stream().forEach(System.out::println);
        strs= DateTimeUtil.calcLongTimeBetween(1528071370000L, 1530663370000L, DateTimeUtil.DEFAULT_DATE_DAY, DateTimeUtil.ONE_DAY);
        strs.stream().forEach(System.out::println);

    }

}

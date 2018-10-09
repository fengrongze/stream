package com.cst.stream.common;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

import java.text.ParseException;

/**
 * @author Johnney.chiu
 * create on 2017/12/19 18:17
 * @Description rowkey生成
 */
public class RowKeyGenerate {

    public static String getRowKeyById(String id, Long time, CstConstants.TIME_SELECT time_select) throws ParseException {
        String headKey=MD5Hash.getMD5AsHex(Bytes.toBytes(id));
        long changeTime = goAndChangeTime(time, time_select);
        String result = String.format("%s_%d", headKey, Long.MAX_VALUE - changeTime);
        return result;
    }

    public static long goAndChangeTime(Long time, CstConstants.TIME_SELECT time_select) throws ParseException {
        long changeTime = 0;
        switch (time_select){
            case DAY:
                changeTime= DateTimeUtil.getDayBase(time);
                break;
            case HOUR:
                changeTime = DateTimeUtil.getHourBase(time);
                break;
            case MONTH:
                changeTime = DateTimeUtil.getMonthBase(time);
                break;
            case YEAR:
                changeTime = DateTimeUtil.getYearBase(time);
                break;

        }
        return changeTime;
    }

    public static String getRowKeyById(String id, Long time, CstConstants.TIME_SELECT time_select,String type) throws ParseException {
        String headKey=MD5Hash.getMD5AsHex(Bytes.toBytes(id));
        long changeTime = goAndChangeTime(time, time_select);
        String result = String.format("%s_%s_%d", headKey,type, Long.MAX_VALUE - changeTime);
        return result;
    }
    public static String getRowKeyById(String id){
        return MD5Hash.getMD5AsHex(Bytes.toBytes(id));
    }

    public static String getRowKeyById(String id,String type){
        String result = String.format("%s_%s",MD5Hash.getMD5AsHex(Bytes.toBytes(id)) , type);
        return result;
    }


}

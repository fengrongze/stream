package com.cst.jstorm.commons.stream.operations.jump;

import com.cst.jstorm.commons.stream.constants.ExceptionCodeStatus;
import com.cst.stream.common.DateTimeUtil;
import com.cst.stream.common.DateTimeUtils;
import com.cst.stream.stathour.CSTData;
import com.cst.stream.stathour.obd.ObdHourSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.math.NumberUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Johnney.Chiu
 * create on 2018/8/31 16:57
 * @Description
 * @title
 */
@Slf4j
public class JumpCharge {

    public static ExceptionCodeStatus jumpNomalChargeWithMoreData(CSTData cstData
            , Map map,int sourceSpeed,Float sourceDistance
            , long latestTime, Float latestTotalDistance){

        if (sourceSpeed<0&&sourceDistance<0) {
            log.warn("speed or totalDistance error,original data {},latest time :{},latest distance:{}",cstData,latestTime,latestTotalDistance);
            return ExceptionCodeStatus.CALC_RETURN;
        }

        float dx = NumberUtils.toFloat((String) map.get("distance_spacing"), 2.0f);
        if(latestTotalDistance>sourceDistance) {
            if (latestTotalDistance - sourceDistance < dx) {
                log.warn("upload  distance spacing exception return,original data {},latest time :{},latest distance:{}", cstData, latestTime, latestTotalDistance);
                return ExceptionCodeStatus.CALC_RETURN;
            } else {
                log.warn("upload  distance spacing exception jump,original data {},latest time :{},latest distance:{}", cstData, latestTime, latestTotalDistance);
                return ExceptionCodeStatus.CALC_JUMP;
            }
        }

        if (cstData.getTime()<=latestTime) {
            log.warn("upload  time exception,original data {},latest time :{},latest distance:{}",cstData,latestTime,latestTotalDistance);
            return ExceptionCodeStatus.CALC_RETURN;
        }
        //如果时间间隔5秒
        if(cstData.getTime()-latestTime>5000) {
            //1000公里每小时
            float maxSpeed = NumberUtils.toFloat((String) map.get("travel_speed"), 1000F);
            BigDecimal upValue = new BigDecimal(cstData.getTime() - latestTime).abs().multiply(new BigDecimal(maxSpeed))
                    .divide(new BigDecimal(3600 * 1000), 3, RoundingMode.HALF_UP);
            BigDecimal carDistance = new BigDecimal(sourceDistance).subtract(new BigDecimal(latestTotalDistance));
            //如果这个数据的最大
            if (upValue.compareTo(carDistance) < 0) {
                log.warn("jump data,original data {},latest time :{},latest distance:{}", cstData, latestTime,latestTotalDistance);
                return ExceptionCodeStatus.CALC_JUMP;
            }
        }else{
            //两个里程差距3公里视为跳变
            if(Math.abs(sourceDistance-latestTotalDistance)>NumberUtils.toFloat((String)map.get("jump_mile"), 3F)){
                log.warn("jump data,original data {},latest time :{},latest distance:{}",cstData,latestTime,latestTotalDistance);
                return ExceptionCodeStatus.CALC_JUMP;
            }
        }


        return ExceptionCodeStatus.CALC_DATA;

    }



    public static ExceptionCodeStatus jumpNomalChargeWithMoreData(CSTData cstData
            , Map map,int sourceSpeed,Double sourceDistance
            , long latestTime, Double latestTotalDistance){

        if (sourceSpeed<0&&sourceDistance<0) {
            log.warn("speed or totalDistance error,original data {},latest time :{},latest distance:{}",cstData,latestTime,latestTotalDistance);
            return ExceptionCodeStatus.CALC_RETURN;
        }

        double dx = NumberUtils.toDouble((String) map.get("distance_spacing"), 2.0D);
        if(latestTotalDistance>sourceDistance) {
            if (latestTotalDistance - sourceDistance < dx) {
                log.warn("upload  distance spacing exception return,original data {},latest time :{},latest distance:{}", cstData, latestTime, latestTotalDistance);
                return ExceptionCodeStatus.CALC_RETURN;
            } else {
                log.warn("upload  distance spacing exception jump,original data {},latest time :{},latest distance:{}", cstData, latestTime, latestTotalDistance);
                return ExceptionCodeStatus.CALC_JUMP;
            }
        }

        if (cstData.getTime()<=latestTime) {
            log.warn("upload  time exception,original data {},latest time :{},latest distance:{}",cstData,latestTime,latestTotalDistance);
            return ExceptionCodeStatus.CALC_RETURN;
        }
        //如果时间间隔5秒
        if(cstData.getTime()-latestTime>5000) {
            //1000公里每小时
            float maxSpeed = NumberUtils.toFloat((String) map.get("travel_speed"), 1000F);
            BigDecimal upValue = new BigDecimal(cstData.getTime() - latestTime).abs().multiply(new BigDecimal(maxSpeed))
                    .divide(new BigDecimal(3600 * 1000), 3, RoundingMode.HALF_UP);
            BigDecimal carDistance = new BigDecimal(sourceDistance).subtract(new BigDecimal(latestTotalDistance));
            //如果这个数据的最大
            if (upValue.compareTo(carDistance) < 0) {
                log.warn("jump data,original data {},latest time :{},latest distance:{}", cstData, latestTime,latestTotalDistance);
                return ExceptionCodeStatus.CALC_JUMP;
            }
        }else{
            //两个里程差距3公里视为跳变
            if(Math.abs(sourceDistance-latestTotalDistance)>NumberUtils.toDouble((String)map.get("jump_mile"), 3D)){
                log.warn("jump data,original data {},latest time :{},latest distance:{}",cstData,latestTime,latestTotalDistance);
                return ExceptionCodeStatus.CALC_JUMP;
            }
        }


        return ExceptionCodeStatus.CALC_DATA;

    }

    public static void main(String...args) throws ParseException {
        System.out.println(jumpNomalChargeWithMoreData(
                new CSTData("9bd756a96e32489383e88420737f5958", DateTimeUtil.strToTimestamp("2018-09-06 07:28:45",DateTimeUtil.DEFAULT_DATE_DEFULT)),
                new HashMap<String,String>(){{put("travel_speed","1000");}},
                0,
                14050.889F,
                DateTimeUtil.strToTimestamp("2018-09-05 14:22:13",DateTimeUtil.DEFAULT_DATE_DEFULT),
                14050.979F
        ));
    }


}

package cst.jstorm.daymonth.calcalations.am;

import com.cst.gdcp.protobuf.common.CSTAlarmTypeConst;
import com.cst.stream.stathour.CSTData;
import com.cst.stream.stathour.am.AmDayLatestData;
import com.cst.stream.stathour.am.AmDayTransfor;
import com.cst.stream.stathour.am.AmHourSource;
import com.cst.stream.stathour.am.AmHourTransfor;
import com.cst.jstorm.commons.stream.constants.ExceptionCodeStatus;
import com.cst.jstorm.commons.stream.operations.DataDealTransforInterface;
import com.cst.jstorm.commons.stream.operations.DataTransforInterface;
import com.cst.stream.stathour.dormancy.DormancySource;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static com.cst.jstorm.commons.stream.operations.InvalidDataCharge.invalidTooLessThanTime;

/**
 * @author Johnney.chiu
 * create on 2017/12/20 11:21
 * @Description Am 天计算
 */
@NoArgsConstructor
@Slf4j
public class AmDayCalcBiz implements
        DataDealTransforInterface<AmDayTransfor,AmHourSource,AmDayLatestData> {

    //点火告警
    private AmDayCalcBiz changeIgnition(AmHourTransfor amHourTransfor, Integer type) {
        if(CSTAlarmTypeConst.ALARM_CAR_IGNITION==type.intValue())
            amHourTransfor.setIgnition(amHourTransfor.getIgnition()+1);
        return this;
    }
    private int changeIgnition(int oldIgnition,Integer type) {
        if(CSTAlarmTypeConst.ALARM_CAR_IGNITION==type.intValue())
            return oldIgnition + 1;
        return oldIgnition;
    }
    //熄火告警
    private AmDayCalcBiz changeFlameout(AmHourTransfor amHourTransfor, Integer type) {
        if(CSTAlarmTypeConst.ALARM_CAR_FLAMEOUT==type.intValue())
            amHourTransfor.setFlameOut(amHourTransfor.getFlameOut()+1);
        return this;
    }
    private int changeFlameout(int oldFlameOut,Integer type) {
        if(CSTAlarmTypeConst.ALARM_CAR_FLAMEOUT==type.intValue())
            return oldFlameOut + 1;
        return oldFlameOut;
    }
    //插入告警
    private AmDayCalcBiz changeInesert(AmHourTransfor amHourTransfor, Integer type) {
        if(CSTAlarmTypeConst.ALARM_TERMINAL_INSERT==type.intValue())
            amHourTransfor.setInsertNum(amHourTransfor.getInsertNum()+1);
        return this;
    }
    private int changeInesert(int oldInsertNum,Integer type) {
        if(CSTAlarmTypeConst.ALARM_TERMINAL_INSERT==type.intValue())
            return oldInsertNum+1;
        return oldInsertNum;
    }
    //疲劳驾驶告警
    private int changeIsfatigue(int oldFatiugue,Integer type) {
        if(CSTAlarmTypeConst.ALARM_FATIGUE_DRIVING_PARA==type.intValue())
            return 1;
        return oldFatiugue;
    }
    private AmDayCalcBiz changeIsfatigue(AmHourTransfor amHourTransfor, Integer type) {
        if(CSTAlarmTypeConst.ALARM_FATIGUE_DRIVING_PARA==type.intValue())
            amHourTransfor.setIsFatigue(1);
        return this;
    }

    //碰撞告警
    private AmDayCalcBiz changeCollision(AmHourTransfor amHourTransfor, Integer type, Integer gatherType) {
        if(gatherType==3) return this;
        if(CSTAlarmTypeConst.ALARM_COLLISION==type.intValue())
            amHourTransfor.setCollision(amHourTransfor.getCollision()+1);
        return this;
    }
    private int changeCollision(int oldCollision,Integer type, Integer gatherType) {
        if(CSTAlarmTypeConst.ALARM_COLLISION==type.intValue())
            if(gatherType!=3) return oldCollision+1;
        return oldCollision;
    }
    //超速告警
    private AmDayCalcBiz changeOverspeed(AmHourTransfor amHourTransfor, Integer type) {
        if(CSTAlarmTypeConst.ALARM_OVERSPEED==type.intValue())
            amHourTransfor.setOverSpeed(amHourTransfor.getOverSpeed()+1);
        return this;
    }
    private int changeOverspeed(int oldOverSpeed,Integer type) {
        if(CSTAlarmTypeConst.ALARM_OVERSPEED==type.intValue())
            return oldOverSpeed+1;
        return oldOverSpeed;
    }
    //失联告警
    private AmDayCalcBiz changeDisappear(AmHourTransfor amHourTransfor, Integer type) {
        if(amHourTransfor.getIsMissing()==1) return this;
        if(CSTAlarmTypeConst.ALARM_DISAPPEAR_STATE==type.intValue())
            amHourTransfor.setIsMissing(amHourTransfor.getIsMissing()+1);
        return this;
    }
    private int changeDisappear(int oldDisAppear,Integer type) {
        if(CSTAlarmTypeConst.ALARM_DISAPPEAR_STATE==type.intValue())
            return 1;
        return oldDisAppear;
    }

    //拔出时长告警
    private AmDayCalcBiz changePullout(AmHourTransfor amHourTransfor, Integer type){
        if(CSTAlarmTypeConst.ALARM_LONG_NOGPSINF_TIME==type.intValue()) {
            amHourTransfor.setPulloutTimes(amHourTransfor.getPulloutTimes() +1);
        }
        return this;
    }
    private float changePulloutTimes(float oldPullOutTimes,Integer type,Long pullout,long obdTime){
        if(CSTAlarmTypeConst.ALARM_TERMINAL_INSERT==type.intValue()) {
            if(pullout==null||pullout<=0)
                return oldPullOutTimes;
            float anotherTimes = (obdTime-pullout.longValue()) / 1000F;
            return oldPullOutTimes+anotherTimes;
        }
        return oldPullOutTimes;
    }
    private int changePulloutCounts(int oldPullOutCounts,Integer type){
        if(CSTAlarmTypeConst.ALARM_TERMINAL_PULL==type.intValue()) {
            return oldPullOutCounts+1;
        }
        return oldPullOutCounts;
    }

    @Override
    public ExceptionCodeStatus commpareExceptionWithEachData(AmHourSource amHourSource, AmDayLatestData latestData, Map map) {
        if (amHourSource.getTime() < latestData.getTime()) {
            log.warn("upload am time exception,original data {},latest data :{}",amHourSource,latestData);
            return ExceptionCodeStatus.CALC_NO_TIME;
        }

        return ExceptionCodeStatus.CALC_DATA;
    }

    @Override
    public AmDayLatestData calcLatestData(AmDayLatestData latestData, AmHourSource amHourSource, Map map,ExceptionCodeStatus status) {
        latestData.setCarId(amHourSource.getCarId());
        if(status!=ExceptionCodeStatus.CALC_NO_TIME)
            latestData.setTime(amHourSource.getTime());
        latestData.setCollision(changeCollision(latestData.getCollision(),amHourSource.getAlarmType(),amHourSource.getGatherType()));
        latestData.setFlameOut(changeFlameout(latestData.getFlameOut(),amHourSource.getAlarmType()));
        latestData.setIgnition(changeIgnition(latestData.getIgnition(),amHourSource.getAlarmType()));
        latestData.setInsertNum(changeInesert(latestData.getInsertNum(),amHourSource.getAlarmType()));
        latestData.setIsFatigue(changeIsfatigue(latestData.getIsFatigue(),amHourSource.getAlarmType()));
        latestData.setIsMissing(changeDisappear(latestData.getIsMissing(),amHourSource.getAlarmType()));
        latestData.setOverSpeed(changeOverspeed(latestData.getOverSpeed(),amHourSource.getAlarmType()));
        //latestData.setPulloutTimes(changePulloutTimes(latestData.getPulloutTimes(),amHourSource.getAlarmType(),amHourSource.getTroubleCode()));
        latestData.setPulloutTimes(changePulloutTimes(latestData.getPulloutTimes(),amHourSource.getAlarmType()
                ,amHourSource.getPullout(),amHourSource.getTime()));
        latestData.setPulloutCounts(changePulloutCounts(latestData.getPulloutCounts(),amHourSource.getAlarmType()));
        return latestData;
    }

    @Override
    public AmDayLatestData initLatestData(AmHourSource amHourSource, Map map,AmDayLatestData latestData) {
        return AmDayLatestData.builder()
                .carId(amHourSource.getCarId())
                .time(amHourSource.getTime())
                .collision(changeCollision(0,amHourSource.getAlarmType(),amHourSource.getGatherType()))
                .flameOut(changeFlameout(0,amHourSource.getAlarmType()))
                .ignition(changeIgnition(0,amHourSource.getAlarmType()))
                .insertNum(changeInesert(0,amHourSource.getAlarmType()))
                .isFatigue(changeIsfatigue(0,amHourSource.getAlarmType()))
                .isMissing(changeDisappear(0,amHourSource.getAlarmType()))
                .overSpeed(changeOverspeed(0,amHourSource.getAlarmType()))
                .pulloutTimes(changePulloutTimes(0f,amHourSource.getAlarmType(),amHourSource.getPullout(),amHourSource.getTime()))
                .pulloutCounts(changePulloutCounts(0,amHourSource.getAlarmType()))
                .build();
    }

    @Override
    public AmDayTransfor calcTransforData(AmHourSource latestFirstData, AmDayLatestData latestData, AmHourSource amHourSource, Map map) {
        return AmDayTransfor.builder()
                .carId(latestFirstData.getCarId())
                .time(latestFirstData.getTime())
                .collision(latestData.getCollision())
                .flameOut(latestData.getFlameOut())
                .ignition(latestData.getIgnition())
                .insertNum(latestData.getInsertNum())
                .isFatigue(latestData.getIsFatigue())
                .isMissing(latestData.getIsMissing())
                .overSpeed(latestData.getOverSpeed())
                .pulloutTimes(latestData.getPulloutTimes())
                .pulloutCounts(latestData.getPulloutCounts())
                .build();
    }

    @Override
    public AmDayTransfor initFromTempTransfer(AmDayLatestData latestData,AmHourSource amHourSource, Map map,long supplyTime) {
        return AmDayTransfor.builder()
                .carId(latestData.getCarId())
                .time(supplyTime)
                .pulloutTimes(0f)
                .overSpeed(0)
                .isMissing(0)
                .isFatigue(0)
                .insertNum(0)
                .ignition(0)
                .flameOut(0)
                .collision(0)
                .pulloutCounts(0)
                .build();
    }

    @Override
    public Map<String, String> convertData2Map(AmDayTransfor transfor,AmDayLatestData latestData) {
        Map<String,String> map = null;
        if(null==transfor) {
            transfor = initTransfor();
         }
            map = new HashMap<>();
            map.put("carId",transfor.getCarId());
            map.put("time",null!=transfor.getTime()?String.valueOf(transfor.getTime()):null);
            map.put("ignition",String.valueOf(transfor.getIgnition()));
            map.put("flameOut",String.valueOf(transfor.getFlameOut()));
            map.put("insertNum",String.valueOf(transfor.getInsertNum()));
            map.put("collision",String.valueOf(transfor.getCollision()));
            map.put("overSpeed",String.valueOf(transfor.getOverSpeed()));
            map.put("isMissing",String.valueOf(transfor.getIsMissing()));
            map.put("pulloutTimes",String.valueOf(transfor.getPulloutTimes()));
            map.put("isFatigue",String.valueOf(transfor.getIsFatigue()));
        return map;
    }

    private AmDayTransfor initTransfor() {
        return AmDayTransfor.builder()
                .carId(null)
                .time(null)
                .collision(0)
                .flameOut(0)
                .ignition(0)
                .insertNum(0)
                .isFatigue(0)
                .isMissing(0)
                .overSpeed(0)
                .pulloutTimes(0F)
                .pulloutCounts(0)
                .build();
    }

    @Override
    public AmDayTransfor calcIntegrityData(AmHourSource latestFirstData, AmDayLatestData latestData,AmDayTransfor transfor, Map map) {
        return AmDayTransfor.builder()
                .carId(latestFirstData.getCarId())
                .time(latestFirstData.getTime())
                .collision(latestData.getCollision())
                .flameOut(latestData.getFlameOut())
                .ignition(latestData.getIgnition())
                .insertNum(latestData.getInsertNum())
                .isFatigue(latestData.getIsFatigue())
                .isMissing(latestData.getIsMissing())
                .overSpeed(latestData.getOverSpeed())
                .pulloutTimes(latestData.getPulloutTimes())
                .pulloutCounts(latestData.getPulloutCounts())
                .build();
    }


    @Override
    public AmDayTransfor initFromDormancy(AmDayLatestData latestData,long supplyTime) {
        return initFromTempTransfer(latestData,null,null,supplyTime);
    }
}

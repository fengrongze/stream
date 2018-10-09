package cst.jstorm.hour.calcalations.am;

import com.cst.gdcp.protobuf.common.CSTAlarmTypeConst;
import com.cst.jstorm.commons.stream.constants.ExceptionCodeStatus;
import com.cst.jstorm.commons.stream.operations.DataDealTransforInterface;
import com.cst.jstorm.commons.stream.operations.DataTransforInterface;
import com.cst.stream.stathour.CSTData;
import com.cst.stream.stathour.am.AmHourLatestData;
import com.cst.stream.stathour.am.AmHourSource;
import com.cst.stream.stathour.am.AmHourTransfor;
import com.cst.stream.stathour.dormancy.DormancySource;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

import static com.cst.jstorm.commons.stream.operations.InvalidDataCharge.invalidTooLessThanTime;

/**
 * @author Johnney.chiu
 * create on 2017/12/20 11:21
 * @Description Am 小时计算
 */
@NoArgsConstructor
@Slf4j
public class AmHourCalcBiz implements DataTransforInterface<AmHourTransfor,AmHourSource>,
        DataDealTransforInterface<AmHourTransfor,AmHourSource,AmHourLatestData> {

    @Override
    public void execute(AmHourTransfor amHourTransfor,AmHourSource amHourSource,Map other) {
        log.debug(amHourSource.toString());
        log.debug(amHourTransfor.toString());
        try {
            log.debug("am before transfor hour data:{} {} {}", amHourSource.getCarId(),amHourSource.getTime(),amHourSource.toString());

            changeIgnition(amHourTransfor, amHourSource.getAlarmType())
                    .changeCollision(amHourTransfor, amHourSource.getAlarmType(), amHourSource.getGatherType())
                    .changeDisappear(amHourTransfor, amHourSource.getAlarmType())
                    .changeFlameout(amHourTransfor, amHourSource.getAlarmType())
                    .changeInesert(amHourTransfor, amHourSource.getAlarmType())
                    .changeOverspeed(amHourTransfor, amHourSource.getAlarmType())
                    .changeIsfatigue(amHourTransfor, amHourSource.getAlarmType())
                    .changePullout(amHourTransfor, amHourSource.getAlarmType());

            log.debug("am after transfor hour data:{} {} {}", amHourTransfor.getCarId(),amHourTransfor.getTime(),amHourTransfor.toString());
        }catch (Exception e){
            log.error("Exception :{},data is{}",e,amHourSource.toString());
        }
    }
    @Override
    public AmHourTransfor init(AmHourSource amHourSource, Map<String,Object> other) {
        return new AmHourTransfor(amHourSource.getCarId(),(Long)other.get("uploadTime"), 0,0,0,0,0,0
                ,0f,0,0);
    }
    @Override
    public AmHourTransfor initOffet(AmHourTransfor amHourTransfor, AmHourSource amHourSource, Map<String,Object> other) {
        return init(amHourSource,other);
    }

    @Override
    public AmHourTransfor initFromTransfer(AmHourTransfor amHourTransfor, Map<String, Object> other) {
        return new AmHourTransfor(amHourTransfor.getCarId(),(Long)other.get("uploadTime"), 0,0,0,0,0,0
                ,0f,0,0);
    }



    //点火告警
    private AmHourCalcBiz changeIgnition(AmHourTransfor amHourTransfor,Integer type) {
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
    private AmHourCalcBiz changeFlameout(AmHourTransfor amHourTransfor,Integer type) {
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
    private AmHourCalcBiz changeInesert(AmHourTransfor amHourTransfor,Integer type) {
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
    private AmHourCalcBiz changeIsfatigue(AmHourTransfor amHourTransfor,Integer type) {
        if(CSTAlarmTypeConst.ALARM_FATIGUE_DRIVING_PARA==type.intValue())
            amHourTransfor.setIsFatigue(1);
        return this;
    }

    //碰撞告警
    private AmHourCalcBiz changeCollision(AmHourTransfor amHourTransfor,Integer type, Integer gatherType) {
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
    private AmHourCalcBiz changeOverspeed(AmHourTransfor amHourTransfor,Integer type) {
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
    private AmHourCalcBiz changeDisappear(AmHourTransfor amHourTransfor,Integer type) {
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
    private AmHourCalcBiz changePullout(AmHourTransfor amHourTransfor,Integer type){
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
    public ExceptionCodeStatus commpareExceptionWithEachData(AmHourSource amHourSource, AmHourLatestData latestData, Map map) {
        if (amHourSource.getTime() < latestData.getTime()) {
            log.warn("upload am time exception,original data {},latest data :{}",amHourSource,latestData);
            return ExceptionCodeStatus.CALC_NO_TIME;
        }
        if (invalidTooLessThanTime(amHourSource.getTime())) {
            log.warn("upload am time too less than now:original data {},latest data {}", amHourSource, latestData);
            return ExceptionCodeStatus.CALC_RETURN;
        }
        return ExceptionCodeStatus.CALC_DATA;
    }

    @Override
    public AmHourLatestData calcLatestData(AmHourLatestData hourLatestData, AmHourSource amHourSource, Map map,ExceptionCodeStatus status) {
        hourLatestData.setCarId(amHourSource.getCarId());
        if(status!=ExceptionCodeStatus.CALC_NO_TIME)
            hourLatestData.setTime(amHourSource.getTime());
        hourLatestData.setCollision(changeCollision(hourLatestData.getCollision(),amHourSource.getAlarmType(),amHourSource.getGatherType()));
        hourLatestData.setFlameOut(changeFlameout(hourLatestData.getFlameOut(),amHourSource.getAlarmType()));
        hourLatestData.setIgnition(changeIgnition(hourLatestData.getIgnition(),amHourSource.getAlarmType()));
        hourLatestData.setInsertNum(changeInesert(hourLatestData.getInsertNum(),amHourSource.getAlarmType()));
        hourLatestData.setIsFatigue(changeIsfatigue(hourLatestData.getIsFatigue(),amHourSource.getAlarmType()));
        hourLatestData.setIsMissing(changeDisappear(hourLatestData.getIsMissing(),amHourSource.getAlarmType()));
        hourLatestData.setOverSpeed(changeOverspeed(hourLatestData.getOverSpeed(),amHourSource.getAlarmType()));
        hourLatestData.setPulloutTimes(changePulloutTimes(hourLatestData.getPulloutTimes(),amHourSource.getAlarmType()
                ,amHourSource.getPullout(),amHourSource.getTime()));
        hourLatestData.setPulloutCounts(changePulloutCounts(hourLatestData.getPulloutCounts(),amHourSource.getAlarmType()));
        return hourLatestData;
    }

    @Override
    public AmHourLatestData initLatestData(AmHourSource amHourSource, Map map,AmHourLatestData latestData) {
        return AmHourLatestData.builder()
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
    public AmHourTransfor calcTransforData(AmHourSource latestFirstData, AmHourLatestData latestData, AmHourSource amHourSource, Map map) {
        return AmHourTransfor.builder()
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
    public AmHourTransfor initFromTempTransfer(AmHourLatestData latestData,AmHourSource amHourSource, Map map,long supplyTime) {
        return AmHourTransfor.builder()
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
    public Map<String, String> convertData2Map(AmHourTransfor transfor,AmHourLatestData latestData) {
        return null;
    }

    @Override
    public AmHourTransfor calcIntegrityData(AmHourSource latestFirstData, AmHourLatestData latestData,AmHourTransfor amHourTransfor, Map map) {
        return null;
    }

    @Override
    public AmHourTransfor initFromDormancy(AmHourLatestData latestData, long l1) {
        return null;
    }
}

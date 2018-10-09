package com.cst.bigdata.service.integrate;

import com.cst.stream.base.CodeStatus;
import com.cst.stream.common.*;
import com.cst.stream.stathour.CSTData;
import com.cst.stream.stathour.CstStreamBaseResult;
import com.cst.stream.stathour.am.AmHourLatestData;
import com.cst.stream.stathour.am.AmHourTransfor;
import com.cst.stream.stathour.de.DeHourLatestData;
import com.cst.stream.stathour.de.DeHourSource;
import com.cst.stream.stathour.de.DeHourTransfor;
import com.cst.stream.stathour.gps.GpsHourLatestData;
import com.cst.stream.stathour.gps.GpsHourTransfor;
import com.cst.stream.stathour.integrated.HourIntegratedTransfor;
import com.cst.stream.stathour.mileage.MileageHourLatestData;
import com.cst.stream.stathour.mileage.MileageHourSource;
import com.cst.stream.stathour.mileage.MileageHourTransfor;
import com.cst.stream.stathour.obd.ObdHourLatestData;
import com.cst.stream.stathour.obd.ObdHourSource;
import com.cst.stream.stathour.obd.ObdHourTransfor;
import com.cst.stream.stathour.trace.TraceHourLatestData;
import com.cst.stream.stathour.trace.TraceHourTransfor;
import com.cst.stream.stathour.tracedelete.TraceDeleteHourLatestData;
import com.cst.stream.stathour.tracedelete.TraceDeleteHourTransfor;
import com.cst.stream.stathour.voltage.VoltageHourLatestData;
import com.cst.stream.stathour.voltage.VoltageHourTransfor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.math.BigDecimal;

import static com.cst.stream.common.BusinessMathUtil.calcAvarageSpeed;
import static com.cst.stream.common.BusinessMathUtil.calcFuelPerHundred;
import static com.cst.stream.common.MathUtils.round;

/**
 * @author Johnney.chiu
 * create on 2018/3/6 14:54
 * @Description 整合的天数据整理
 */
@Service
@Slf4j
public class CstStreamHourNoDelayIntegrateService<S extends CSTData,T extends CSTData,L extends CSTData> {

    @Autowired
    private RedisCommonService<L,S> redisCommonService;

    @Autowired
    private CstStreamHourIntegrateService<T> cstStreamHourIntegrateService;

    @Autowired
    private CstStreamHourFirstIntegrateService<S> cstStreamHourFirstIntegrateService;




    public CstStreamBaseResult<AmHourTransfor> getAmNoDelayHourTransfor(String carId,String head){
        AmHourLatestData amHourLatestData;
            amHourLatestData= (AmHourLatestData) redisCommonService.getLatestDataFromRedis(head, carId);

        if (amHourLatestData==null) {
            return  CstStreamBaseResult.fail(CodeStatus.GENERAL_EXCEPTION_CODE, "can not find latest data");
        }

        AmHourTransfor amHourTransfor = AmHourTransfor.builder()
                .collision(amHourLatestData.getCollision())
                .flameOut(amHourLatestData.getFlameOut())
                .ignition(amHourLatestData.getIgnition())
                .insertNum(amHourLatestData.getInsertNum())
                .isFatigue(amHourLatestData.getIsFatigue())
                .overSpeed(amHourLatestData.getOverSpeed())
                .isMissing(amHourLatestData.getIsMissing())
                .time(amHourLatestData.getTime())
                .carId(amHourLatestData.getCarId())
                .build();

       return CstStreamBaseResult.success(amHourTransfor);
    }

    public CstStreamBaseResult<MileageHourTransfor> getMileageNoDelayHourTransfor(String carId, String type, String head, String[] sourceColumns, Class<?> sourceClazz) {
        MileageHourLatestData latestData;
        latestData= (MileageHourLatestData) redisCommonService.getLatestDataFromRedis(head, carId);

        if (latestData==null) {
            return  CstStreamBaseResult.fail(CodeStatus.GENERAL_EXCEPTION_CODE, "can not find latest data");
        }

        CstStreamBaseResult<MileageHourSource>  mileageFirstDaySources = (CstStreamBaseResult<MileageHourSource>)cstStreamHourFirstIntegrateService
                .getHourSource(carId, latestData.getTime(), type, sourceColumns, sourceClazz);
        if (mileageFirstDaySources==null||mileageFirstDaySources.getData() == null) {
            return  CstStreamBaseResult.fail(CodeStatus.GENERAL_EXCEPTION_CODE, "can not find first data");
        }
        MileageHourSource hourSource = mileageFirstDaySources.getData();
        double fuel =  round(new BigDecimal(latestData.getMilTotalFuel() - hourSource.getMilTotalFuel()), 3);
        double fee = round(new BigDecimal(fuel)
                .multiply(new BigDecimal(redisCommonService.calcPrice(latestData.getCarId(), latestData.getTime()))), 3);
        double gpsMil=  round(BigDecimal.valueOf(latestData.getMilGpsTotalDistance())
                .subtract(BigDecimal.valueOf(hourSource.getMilGpsTotalDistance())), 3);
        double obdMil=  round(BigDecimal.valueOf(latestData.getMilObdTotalDistance())
                .subtract(BigDecimal.valueOf(hourSource.getMilObdTotalDistance())), 3);
        double panelMil=  round(BigDecimal.valueOf(latestData.getPanelDistance())
                .subtract(BigDecimal.valueOf(hourSource.getPanelDistance())), 3);
        MileageHourTransfor mileageDayTransfor = MileageHourTransfor.builder()
                .carId(latestData.getCarId())
                .time(latestData.getTime())
                .gpsSpeed(mileageFirstDaySources.getData().getGpsSpeed())
                .milDuration((int)(hourSource.getMilRunTotalTime()-latestData.getMilRunTotalTime()))
                .milFee(fee)
                .milFuel(fuel)
                .milGpsMaxSpeed((float)Math.max(hourSource.getGpsSpeed(),latestData.getGpsSpeed()))
                .milGpsMileage(gpsMil)
                .milGpsTotalDistance(hourSource.getMilGpsTotalDistance())
                .milObdMaxSpeed((float)Math.max(hourSource.getObdSpeed(),latestData.getObdSpeed()))
                .milObdMileage(obdMil)
                .milObdTotalDistance(hourSource.getMilObdTotalDistance())
                .milPanelMileage(panelMil)
                .milRunTotalTime(hourSource.getMilRunTotalTime())
                .milTotalFuel(hourSource.getMilTotalFuel())
                .obdSpeed(hourSource.getObdSpeed())
                .panelDistance(hourSource.getPanelDistance())
                .build();


        return CstStreamBaseResult.success(mileageDayTransfor);
    }


    public CstStreamBaseResult<DeHourTransfor> getDeNoDelayHourTransfor(String carId ,String type,String head,
                                                                        String[] sourceColumns,Class<?> sourceClazz){
        DeHourLatestData deHourLatestData;
        deHourLatestData= (DeHourLatestData) redisCommonService.getLatestDataFromRedis(head, carId);

        if (deHourLatestData==null) {
            return  CstStreamBaseResult.fail(CodeStatus.GENERAL_EXCEPTION_CODE, "can not find latest data");
        }

        CstStreamBaseResult<DeHourSource>  deFirstHourSources = (CstStreamBaseResult<DeHourSource>)cstStreamHourFirstIntegrateService.getHourSource(carId, deHourLatestData.getTime(), type, sourceColumns, sourceClazz);
        if (deFirstHourSources.getData() == null) {
            return  CstStreamBaseResult.fail(CodeStatus.GENERAL_EXCEPTION_CODE, "can not find first data");
        }

        DeHourTransfor deHourTransfor = DeHourTransfor.builder()
                .rapidDecelerationCount(deHourLatestData.getRapidDecelerationCounts()-deFirstHourSources.getData().getRapidDecelerationCounts())
                .rapidAccelerationCount(deHourLatestData.getRapidAccelerationCounts()-deFirstHourSources.getData().getRapidAccelerationCounts())
                .sharpTurnCount(deHourLatestData.getSharpTurnCounts()-deFirstHourSources.getData().getSharpTurnCounts())
                .time(deHourLatestData.getTime())
                .carId(deHourLatestData.getCarId())
                .build();

        return CstStreamBaseResult.success(deHourTransfor);
    }



    public CstStreamBaseResult<GpsHourTransfor> getGpsNoDelayHourTransfor(String carId,String head){
        GpsHourLatestData gpsHourLatestData;
        gpsHourLatestData= (GpsHourLatestData) redisCommonService.getLatestDataFromRedis(head, carId);

        if (gpsHourLatestData==null) {
            return  CstStreamBaseResult.fail(CodeStatus.GENERAL_EXCEPTION_CODE, "can not find latest data");
        }

        GpsHourTransfor gpsHourTransfor = GpsHourTransfor.builder()
                .gpsCount(gpsHourLatestData.getGpsCount())
                .isNonLocal(gpsHourLatestData.getIsNonLocal())
                .maxSatelliteNum(gpsHourLatestData.getMaxSatelliteNum())
                .time(gpsHourLatestData.getTime())
                .carId(gpsHourLatestData.getCarId())
                .build();

        return CstStreamBaseResult.success(gpsHourTransfor);
    }



    public CstStreamBaseResult<ObdHourTransfor> getObdNoDelayHourTransfor(String carId ,String type,String head,String[] sourceColumns,
                                                                          Class<?> sourceClazz){
        ObdHourLatestData obdHourLatestData;
        obdHourLatestData= (ObdHourLatestData) redisCommonService.getLatestDataFromRedis(head, carId);

        if (obdHourLatestData==null) {
            return  CstStreamBaseResult.fail(CodeStatus.GENERAL_EXCEPTION_CODE, "can not find latest data");
        }

        CstStreamBaseResult<ObdHourSource> obdFirstHourSources = (CstStreamBaseResult<ObdHourSource>)cstStreamHourFirstIntegrateService.getHourSource(carId, obdHourLatestData.getTime(), type, sourceColumns, sourceClazz);
        if (obdFirstHourSources == null||obdFirstHourSources.getData()==null) {
            return  CstStreamBaseResult.fail(CodeStatus.GENERAL_EXCEPTION_CODE, "can not find first data");
        }

        float mileage = (float) round(new BigDecimal(obdHourLatestData.getTotalDistance() - obdFirstHourSources.getData().getTotalDistance()), 3);
        float fuel = (float) round(new BigDecimal(obdHourLatestData.getTotalFuel() - obdFirstHourSources.getData().getTotalFuel()), 3);
        int duration = obdHourLatestData.getRunTotalTime() - obdFirstHourSources.getData().getRunTotalTime();
        ObdHourTransfor obdHourTransfor = ObdHourTransfor.builder()
                .din(obdHourLatestData.getDin())
                .duration(duration)
                .fuel(fuel)
                .isDrive(obdHourLatestData.getIsDrive())
                .isHighSpeed(obdHourLatestData.getIsHighSpeed())
                .maxSpeed(obdHourLatestData.getMaxSpeed())
                .mileage(mileage)
                .motormeterDistance(obdHourLatestData.getMotormeterDistance())
                .runTotalTime(obdHourLatestData.getRunTotalTime())
                .speed(obdHourLatestData.getSpeed())
                .totalDistance(obdHourLatestData.getTotalDistance())
                .totalFuel(obdHourLatestData.getTotalFuel())
                .fee((float)round(new BigDecimal(obdHourLatestData.getTotalFuel()-obdFirstHourSources.getData().getTotalFuel())
                        .multiply(new BigDecimal(redisCommonService.calcPrice(obdHourLatestData.getCarId(),obdHourLatestData.getTime()))),3))
                .fuelPerHundred(calcAvarageSpeed(mileage,duration))
                .toolingProbability(obdHourLatestData.getToolingProbability())
                .dinChange(obdHourLatestData.getDinChange())
                .averageSpeed(calcAvarageSpeed(mileage,duration))
                .time(obdHourLatestData.getTime())
                .carId(obdHourLatestData.getCarId())
                .build();

        return CstStreamBaseResult.success(obdHourTransfor);
    }

    public CstStreamBaseResult<TraceHourTransfor> getTraceNoDelayHourTransfor(String carId,String head){
        TraceHourLatestData traceHourLatestData;
        traceHourLatestData= (TraceHourLatestData) redisCommonService.getLatestDataFromRedis(head, carId);
        if (traceHourLatestData==null) {
            return  CstStreamBaseResult.fail(CodeStatus.GENERAL_EXCEPTION_CODE, "can not find latest data");
        }

        TraceHourTransfor traceHourTransfor= TraceHourTransfor.builder()
                .traceCounts(CollectionUtils.isNotEmpty(traceHourLatestData.getTraces())?traceHourLatestData.getTraces().size():0)
                .time(traceHourLatestData.getTime())
                .carId(traceHourLatestData.getCarId())
                .build();

        return CstStreamBaseResult.success(traceHourTransfor);
    }



    public CstStreamBaseResult<TraceDeleteHourTransfor> getTraceDeleteNoDelayHourTransfor(String carId,String head){
        TraceDeleteHourLatestData traceDeleteHourLatestData;
        traceDeleteHourLatestData= (TraceDeleteHourLatestData) redisCommonService.getLatestDataFromRedis(head, carId);

        if (traceDeleteHourLatestData==null) {
            return  CstStreamBaseResult.fail(CodeStatus.GENERAL_EXCEPTION_CODE, "can not find latest data");
        }

        TraceDeleteHourTransfor traceDeleteHourTransfor= TraceDeleteHourTransfor.builder()
                .traceDeleteCounts(traceDeleteHourLatestData.getTraceDeleteCounts())
                .time(traceDeleteHourLatestData.getTime())
                .carId(traceDeleteHourLatestData.getCarId())
                .build();

        return CstStreamBaseResult.success(traceDeleteHourTransfor);
    }



    public CstStreamBaseResult<VoltageHourTransfor> getHourNoDelayDataHourTransfor(String carId, String head){
        VoltageHourLatestData voltageHourLatestData;
        voltageHourLatestData= (VoltageHourLatestData) redisCommonService.getLatestDataFromRedis(head, carId);
        if (voltageHourLatestData==null) {
            return  CstStreamBaseResult.fail(CodeStatus.GENERAL_EXCEPTION_CODE, "can not find latest data");
        }

        VoltageHourTransfor voltageHourTransfor= VoltageHourTransfor.builder()
                .maxVoltage(voltageHourLatestData.getMaxVoltage())
                .minVoltage(voltageHourLatestData.getMinVoltage())
                .time(voltageHourLatestData.getTime())
                .carId(voltageHourLatestData.getCarId())
                .build();

        return CstStreamBaseResult.success(voltageHourTransfor);
    }


    public CstStreamBaseResult<HourIntegratedTransfor> getHourNoDelayDataHourTransfor(String carId){
        long nowTime = System.currentTimeMillis();
        HourIntegratedTransfor hourIntegratedTransfor=HourIntegratedTransfor.builder().carId(carId).time(nowTime)
                .averageSpeed(0f)
                .collision(0)
                .din(null)
                .dinChange(0)
                .duration(0)
                .fee(0f)
                .flameOut(0)
                .fuel(0f)
                .fuelPerHundred(0f)
                .gpsCount(0)
                .ignition(0)
                .insertNum(0)
                .isDrive(0)
                .isFatigue(0)
                .isHighSpeed(0)
                .isMissing(0)
                .isNonLocal(0)
                .maxSatelliteNum(0)
                .maxSpeed(0f)
                .maxVoltage(0f)
                .mileage(0f)
                .minVoltage(0f)
                .motormeterDistance(0f)
                .overSpeed(0)
                .pulloutCounts(0)
                .pulloutTimes(0f)
                .rapidAccelerationCount(0)
                .rapidDecelerationCount(0)
                .runTotalTime(0)
                .sharpTurnCount(0)
                .speed(0)
                .toolingProbability(0f)
                .totalDistance(0f)
                .totalFuel(0f)
                .traceCounts(0)
                .traceDeleteCounts(0)
                .powerConsumption(-1F)
                .milDuration(0)
                .milFee(0D)
                .milFuel(0D)
                .milGpsMaxSpeed(0f)
                .milGpsMileage(0D)
                .milObdMaxSpeed(0f)
                .milObdMileage(0D)
                .milPanelMileage(0D)
                .gpsSpeed(0)
                .milGpsTotalDistance(0D)
                .milObdTotalDistance(0D)
                .milRunTotalTime(0L)
                .milTotalFuel(0D)
                .obdSpeed(0)
                .panelDistance(0D)
                .build();

        VoltageHourLatestData voltageHourLatestData;
        AmHourLatestData amHourLatestData;
        ObdHourLatestData obdHourLatestData;
        GpsHourLatestData gpsHourLatestData;
        DeHourLatestData deHourLatestData;
        TraceHourLatestData traceHourLatestData;
        TraceDeleteHourLatestData traceDeleteHourLatestData;
        MileageHourLatestData mileageHourLatestData;


        voltageHourLatestData= (VoltageHourLatestData) redisCommonService.getLatestDataFromRedis(StreamRedisConstants.HourKey.HOUR_VOLTAGE, carId);
        amHourLatestData= (AmHourLatestData) redisCommonService.getLatestDataFromRedis(StreamRedisConstants.HourKey.HOUR_AM, carId);
        obdHourLatestData= (ObdHourLatestData) redisCommonService.getLatestDataFromRedis(StreamRedisConstants.HourKey.HOUR_OBD, carId);
        gpsHourLatestData= (GpsHourLatestData) redisCommonService.getLatestDataFromRedis(StreamRedisConstants.HourKey.HOUR_GPS, carId);
        deHourLatestData= (DeHourLatestData) redisCommonService.getLatestDataFromRedis(StreamRedisConstants.HourKey.HOUR_DE, carId);
        traceHourLatestData= (TraceHourLatestData) redisCommonService.getLatestDataFromRedis(StreamRedisConstants.HourKey.HOUR_TRACE, carId);
        traceDeleteHourLatestData= (TraceDeleteHourLatestData) redisCommonService.getLatestDataFromRedis(StreamRedisConstants.HourKey.HOUR_TRACE_DELETE, carId);
        mileageHourLatestData= (MileageHourLatestData) redisCommonService.getLatestDataFromRedis(StreamRedisConstants.HourKey.HOUR_MILEAGE, carId);



        if (voltageHourLatestData != null
                && DateTimeUtil.dateTimeDifferentBetween(nowTime,voltageHourLatestData.getTime(),DateTimeUtil.DEFAULT_DATE_HOUR)) {
            hourIntegratedTransfor.setMinVoltage(voltageHourLatestData.getMinVoltage());
            hourIntegratedTransfor.setMaxVoltage(voltageHourLatestData.getMaxVoltage());
            hourIntegratedTransfor.setTime(voltageHourLatestData.getTime());
        }
        if (amHourLatestData != null
                && DateTimeUtil.dateTimeDifferentBetween(nowTime,amHourLatestData.getTime(),DateTimeUtil.DEFAULT_DATE_HOUR)) {
            hourIntegratedTransfor.setCollision(amHourLatestData.getCollision());
            hourIntegratedTransfor.setFlameOut(amHourLatestData.getFlameOut());
            hourIntegratedTransfor.setIgnition(amHourLatestData.getIgnition());
            hourIntegratedTransfor.setInsertNum(amHourLatestData.getInsertNum());
            hourIntegratedTransfor.setIsFatigue(amHourLatestData.getIsFatigue());
            hourIntegratedTransfor.setOverSpeed(amHourLatestData.getOverSpeed());
            hourIntegratedTransfor.setIsMissing(amHourLatestData.getIsMissing());
            hourIntegratedTransfor.setPulloutCounts(amHourLatestData.getPulloutCounts());
            hourIntegratedTransfor.setPulloutTimes(amHourLatestData.getPulloutTimes());
            if(amHourLatestData.getTime()!=nowTime && amHourLatestData.getTime()>hourIntegratedTransfor.getTime())
                hourIntegratedTransfor.setTime(amHourLatestData.getTime());
        }
        if(obdHourLatestData != null){
            hourIntegratedTransfor.setDin(obdHourLatestData.getDin());
            hourIntegratedTransfor.setMotormeterDistance(obdHourLatestData.getMotormeterDistance());
            hourIntegratedTransfor.setRunTotalTime(obdHourLatestData.getRunTotalTime());
            hourIntegratedTransfor.setTotalDistance(obdHourLatestData.getTotalDistance());
            hourIntegratedTransfor.setTotalFuel(obdHourLatestData.getTotalFuel());
        }
        if (obdHourLatestData != null
                && DateTimeUtil.dateTimeDifferentBetween(nowTime,obdHourLatestData.getTime(),DateTimeUtil.DEFAULT_DATE_HOUR)) {
            ObdHourSource obdHourSource = (ObdHourSource)redisCommonService.getFirstDataFromRedis(StreamRedisConstants.HourKey.HOUR_OBD, carId,
                    DateTimeUtil.toLongTimeString(obdHourLatestData.getTime(), DateTimeUtil.DEFAULT_DATE_HOUR));
            if(obdHourSource==null) {
                CstStreamBaseResult<ObdHourSource> obdFirstHourSources = (CstStreamBaseResult<ObdHourSource>) cstStreamHourFirstIntegrateService
                        .getHourSource(carId, obdHourLatestData.getTime(), StreamTypeDefine.OBD_TYPE,
                                HbaseColumn.HourSourceColumn.obdHourColumns, ObdHourSource.class);
                if (obdFirstHourSources != null) {
                    obdHourSource = obdFirstHourSources.getData();
                }
            }
            if (obdHourSource!=null) {
                float mileage = (float) round(new BigDecimal(obdHourLatestData.getTotalDistance() - obdHourSource.getTotalDistance()), 3);
                hourIntegratedTransfor.setMileage(mileage);
                int duration = obdHourLatestData.getRunTotalTime() - obdHourSource.getRunTotalTime();
                hourIntegratedTransfor.setDuration(duration);
                float fuel = (float) round(new BigDecimal(obdHourLatestData.getTotalFuel() - obdHourSource.getTotalFuel()), 3);
                hourIntegratedTransfor.setFuel(fuel);
                hourIntegratedTransfor.setFee((float) round(new BigDecimal(fuel)
                        .multiply(new BigDecimal(redisCommonService.calcPrice(obdHourLatestData.getCarId(), obdHourLatestData.getTime()))), 3));
                hourIntegratedTransfor.setFuelPerHundred(calcFuelPerHundred(fuel,mileage));
                hourIntegratedTransfor.setAverageSpeed(calcAvarageSpeed(mileage, duration));

            }
            hourIntegratedTransfor.setIsDrive(obdHourLatestData.getIsDrive());
            hourIntegratedTransfor.setIsHighSpeed(obdHourLatestData.getIsHighSpeed());
            hourIntegratedTransfor.setMaxSpeed(obdHourLatestData.getMaxSpeed());
            hourIntegratedTransfor.setSpeed(obdHourLatestData.getSpeed());
            hourIntegratedTransfor.setDinChange(obdHourLatestData.getDinChange());
            hourIntegratedTransfor.setToolingProbability(obdHourLatestData.getToolingProbability());
            if(obdHourLatestData.getTime()!=nowTime && obdHourLatestData.getTime()>hourIntegratedTransfor.getTime())
                hourIntegratedTransfor.setTime(obdHourLatestData.getTime());
            hourIntegratedTransfor.setPowerConsumption(-1F);
        }

        if (gpsHourLatestData != null
                && DateTimeUtil.dateTimeDifferentBetween(nowTime,gpsHourLatestData.getTime(),DateTimeUtil.DEFAULT_DATE_HOUR)) {
            hourIntegratedTransfor.setGpsCount(gpsHourLatestData.getGpsCount());
            hourIntegratedTransfor.setIsNonLocal(gpsHourLatestData.getIsNonLocal());
            hourIntegratedTransfor.setMaxSatelliteNum(gpsHourLatestData.getMaxSatelliteNum());
            if(gpsHourLatestData.getTime()!=nowTime && gpsHourLatestData.getTime()>hourIntegratedTransfor.getTime())
                hourIntegratedTransfor.setTime(gpsHourLatestData.getTime());
        }

        if (deHourLatestData != null
                && DateTimeUtil.dateTimeDifferentBetween(nowTime,deHourLatestData.getTime(),DateTimeUtil.DEFAULT_DATE_HOUR)) {
            DeHourSource deHourSource = (DeHourSource)redisCommonService.getFirstDataFromRedis(StreamRedisConstants.HourKey.HOUR_DE, carId,
                    DateTimeUtil.toLongTimeString(deHourLatestData.getTime(), DateTimeUtil.DEFAULT_DATE_HOUR));
            if (deHourSource == null) {
                CstStreamBaseResult<DeHourSource>  deFirstHourSources = (CstStreamBaseResult<DeHourSource>)cstStreamHourFirstIntegrateService
                        .getHourSource(carId, deHourLatestData.getTime(),
                                StreamTypeDefine.OBD_TYPE, HbaseColumn.HourSourceColumn.deHourColumns, DeHourSource.class);
                if (deFirstHourSources != null) {
                    deHourSource = deFirstHourSources.getData();
                }
            }

            if (deHourSource!= null) {
                hourIntegratedTransfor.setRapidDecelerationCount(deHourLatestData.getRapidDecelerationCounts() - deHourSource.getRapidDecelerationCounts());
                hourIntegratedTransfor.setRapidAccelerationCount(deHourLatestData.getRapidAccelerationCounts() - deHourSource.getRapidAccelerationCounts());
                hourIntegratedTransfor.setSharpTurnCount(deHourLatestData.getSharpTurnCounts() - deHourSource.getSharpTurnCounts());
            }
            if(deHourLatestData.getTime()!=nowTime && deHourLatestData.getTime()>hourIntegratedTransfor.getTime())
                hourIntegratedTransfor.setTime(deHourLatestData.getTime());
        }
        if (traceHourLatestData != null
                && DateTimeUtil.dateTimeDifferentBetween(nowTime,traceHourLatestData.getTime(),DateTimeUtil.DEFAULT_DATE_HOUR)) {
            hourIntegratedTransfor.setTraceCounts(CollectionUtils.isNotEmpty(traceHourLatestData.getTraces())?traceHourLatestData.getTraces().size():0);
            if(traceHourLatestData.getTime()!=nowTime && traceHourLatestData.getTime()>hourIntegratedTransfor.getTime())
                hourIntegratedTransfor.setTime(traceHourLatestData.getTime());
        }
        if (traceDeleteHourLatestData != null
                && DateTimeUtil.dateTimeDifferentBetween(nowTime,traceDeleteHourLatestData.getTime(),DateTimeUtil.DEFAULT_DATE_HOUR)) {
            hourIntegratedTransfor.setTraceDeleteCounts(traceDeleteHourLatestData.getTraceDeleteCounts());
            if(traceDeleteHourLatestData.getTime()!=nowTime && traceDeleteHourLatestData.getTime()>hourIntegratedTransfor.getTime())
                hourIntegratedTransfor.setTime(traceDeleteHourLatestData.getTime());
        }

        if (mileageHourLatestData != null) {
            hourIntegratedTransfor.setMilObdTotalDistance(mileageHourLatestData.getMilObdTotalDistance());
            hourIntegratedTransfor.setMilGpsTotalDistance(mileageHourLatestData.getMilGpsTotalDistance());
            hourIntegratedTransfor.setMilRunTotalTime(mileageHourLatestData.getMilRunTotalTime());
            hourIntegratedTransfor.setMilTotalFuel(mileageHourLatestData.getMilTotalFuel());
            hourIntegratedTransfor.setPanelDistance(mileageHourLatestData.getPanelDistance());
            hourIntegratedTransfor.setGpsSpeed(mileageHourLatestData.getGpsSpeed());
            hourIntegratedTransfor.setObdSpeed(mileageHourLatestData.getObdSpeed());
        }

        if (mileageHourLatestData != null
                && DateTimeUtil.dateTimeDifferentBetween(nowTime,mileageHourLatestData.getTime(),DateTimeUtil.DEFAULT_DATE_HOUR)) {
            MileageHourSource mileageHourSource = (MileageHourSource)redisCommonService.getFirstDataFromRedis(StreamRedisConstants.HourKey.HOUR_MILEAGE, carId,
                    DateTimeUtil.toLongTimeString(mileageHourLatestData.getTime(), DateTimeUtil.DEFAULT_DATE_HOUR));
            if (mileageHourSource == null) {
                CstStreamBaseResult<MileageHourSource>  mileageFirstHourSources = (CstStreamBaseResult<MileageHourSource>)cstStreamHourFirstIntegrateService
                        .getHourSource(carId, mileageHourLatestData.getTime(),
                                StreamTypeDefine.MILEAGE_TYPE, HbaseColumn.HourSourceColumn.mileageHourColumns, MileageHourSource.class);
                if (mileageFirstHourSources != null) {
                    mileageHourSource = mileageFirstHourSources.getData();
                }
            }

            if (mileageHourSource != null) {
                double fuel =  round(new BigDecimal(mileageHourLatestData.getMilTotalFuel() - mileageHourSource.getMilTotalFuel()), 3);
                double fee =  round(new BigDecimal(fuel)
                        .multiply(new BigDecimal(redisCommonService.calcPrice(mileageHourLatestData.getCarId(), mileageHourSource.getTime()))), 3);
                double gpsMil=  round(BigDecimal.valueOf(mileageHourLatestData.getMilGpsTotalDistance())
                        .subtract(BigDecimal.valueOf(mileageHourSource.getMilGpsTotalDistance())), 3);
                double obdMil= round(BigDecimal.valueOf(mileageHourLatestData.getMilObdTotalDistance())
                        .subtract(BigDecimal.valueOf(mileageHourSource.getMilObdTotalDistance())), 3);
                double panelMil= round(BigDecimal.valueOf(mileageHourLatestData.getPanelDistance())
                        .subtract(BigDecimal.valueOf(mileageHourSource.getPanelDistance())), 3);
                hourIntegratedTransfor.setMilGpsMaxSpeed((float)Math.max(mileageHourLatestData.getGpsSpeed(),mileageHourSource.getGpsSpeed()));
                hourIntegratedTransfor.setMilObdMaxSpeed((float) Math.max(mileageHourLatestData.getObdSpeed(), mileageHourSource.getObdSpeed()));
                hourIntegratedTransfor.setMilObdMileage(obdMil);
                hourIntegratedTransfor.setMilGpsMileage(gpsMil);
                hourIntegratedTransfor.setMilPanelMileage(panelMil);
                hourIntegratedTransfor.setMilFee(fee);
                hourIntegratedTransfor.setMilDuration((int)(mileageHourLatestData.getMilRunTotalTime()-mileageHourSource.getMilRunTotalTime()));
            }
            if(mileageHourSource.getTime()!=nowTime && mileageHourSource.getTime()>hourIntegratedTransfor.getTime())
                hourIntegratedTransfor.setTime(mileageHourSource.getTime());
        }
        return CstStreamBaseResult.success(hourIntegratedTransfor);
    }


}

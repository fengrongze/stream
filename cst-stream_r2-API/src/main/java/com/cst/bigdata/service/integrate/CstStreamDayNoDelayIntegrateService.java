package com.cst.bigdata.service.integrate;

import com.cst.stream.base.CodeStatus;
import com.cst.stream.common.*;
import com.cst.stream.stathour.CSTData;
import com.cst.stream.stathour.CstStreamBaseResult;
import com.cst.stream.stathour.am.AmDayLatestData;
import com.cst.stream.stathour.am.AmDayTransfor;
import com.cst.stream.stathour.de.DeDayLatestData;
import com.cst.stream.stathour.de.DeDayTransfor;
import com.cst.stream.stathour.de.DeHourSource;
import com.cst.stream.stathour.de.DeHourTransfor;
import com.cst.stream.stathour.gps.GpsDayLatestData;
import com.cst.stream.stathour.gps.GpsDayTransfor;
import com.cst.stream.stathour.integrated.DayIntegratedTransfor;
import com.cst.stream.stathour.mileage.MileageDayLatestData;
import com.cst.stream.stathour.mileage.MileageDayTransfor;
import com.cst.stream.stathour.mileage.MileageHourSource;
import com.cst.stream.stathour.obd.ObdDayLatestData;
import com.cst.stream.stathour.obd.ObdDayTransfor;
import com.cst.stream.stathour.obd.ObdHourSource;
import com.cst.stream.stathour.trace.TraceDayLatestData;
import com.cst.stream.stathour.trace.TraceDayTransfor;
import com.cst.stream.stathour.tracedelete.TraceDeleteDayLatestData;
import com.cst.stream.stathour.tracedelete.TraceDeleteDayTransfor;
import com.cst.stream.stathour.voltage.VoltageDayLatestData;
import com.cst.stream.stathour.voltage.VoltageDayTransfor;
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
 * @author Johnney.Chiu
 * create on 2018/3/6 14:54
 * @Description day no delay integrate service
 * @title
 */
@Service
@Slf4j
public class CstStreamDayNoDelayIntegrateService<S extends CSTData,T extends CSTData,L extends CSTData> {


    @Autowired
    private CstStreamDayFirstIntegrateService<S> cstStreamDayFirstIntegrateService;
    @Autowired
    private RedisCommonService<L,S> redisCommonService;



    public CstStreamBaseResult<AmDayTransfor> getAmNoDelayDayTransfor(String carId,String head) {
        AmDayLatestData amDayLatestData;
        amDayLatestData= (AmDayLatestData) redisCommonService.getLatestDataFromRedis(head, carId);

        if (amDayLatestData==null) {
            return  CstStreamBaseResult.fail(CodeStatus.GENERAL_EXCEPTION_CODE, "can not find latest data");
        }


        AmDayTransfor amDayTransfor = AmDayTransfor.builder()
                .collision(amDayLatestData.getCollision())
                .flameOut(amDayLatestData.getFlameOut())
                .ignition(amDayLatestData.getIgnition())
                .insertNum(amDayLatestData.getInsertNum())
                .isFatigue(amDayLatestData.getIsFatigue())
                .overSpeed(amDayLatestData.getOverSpeed())
                .isMissing(amDayLatestData.getIsMissing())
                .time(amDayLatestData.getTime())
                .carId(amDayLatestData.getCarId())
                .pulloutCounts(amDayLatestData.getPulloutCounts())
                .pulloutTimes(amDayLatestData.getPulloutTimes())
                .build();

        return CstStreamBaseResult.success(amDayTransfor);
    }

    public CstStreamBaseResult<MileageDayTransfor> getMileageNoDelayDayTransfor(String carId, String type, String head, String[] sourceColumns, Class<?> sourceClazz) {
        MileageDayLatestData latestData;
        latestData= (MileageDayLatestData) redisCommonService.getLatestDataFromRedis(head, carId);

        if (latestData==null) {
            return  CstStreamBaseResult.fail(CodeStatus.GENERAL_EXCEPTION_CODE, "can not find latest data");
        }

        CstStreamBaseResult<MileageHourSource>  mileageFirstDaySources = (CstStreamBaseResult<MileageHourSource>)cstStreamDayFirstIntegrateService
                .getDaySource(carId, latestData.getTime(), type, sourceColumns, sourceClazz);
        if (mileageFirstDaySources==null||mileageFirstDaySources.getData() == null) {
            return  CstStreamBaseResult.fail(CodeStatus.GENERAL_EXCEPTION_CODE, "can not find first data");
        }
        MileageHourSource hourSource = mileageFirstDaySources.getData();
        double fuel =  round(new BigDecimal(latestData.getMilTotalFuel() - hourSource.getMilTotalFuel()), 3);
        double fee =   round(new BigDecimal(fuel)
                .multiply(new BigDecimal(redisCommonService.calcPrice(latestData.getCarId(), latestData.getTime()))), 3);
        double gpsMil=  round(BigDecimal.valueOf(latestData.getMilGpsTotalDistance())
                .subtract(BigDecimal.valueOf(hourSource.getMilGpsTotalDistance())), 3);
        double obdMil=  round(BigDecimal.valueOf(latestData.getMilObdTotalDistance())
                .subtract(BigDecimal.valueOf(hourSource.getMilObdTotalDistance())), 3);
        double panelMil= round(BigDecimal.valueOf(latestData.getPanelDistance())
                .subtract(BigDecimal.valueOf(hourSource.getPanelDistance())), 3);
        MileageDayTransfor mileageDayTransfor = MileageDayTransfor.builder()
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


    public CstStreamBaseResult<DeDayTransfor> getDeNoDelayDayTransfor(String carId,String type,String head,String[] sourceColumns, Class<?> sourceClazz) {
        DeDayLatestData deDayLatestData;
        deDayLatestData= (DeDayLatestData) redisCommonService.getLatestDataFromRedis(head, carId);

        if (deDayLatestData==null) {
            return  CstStreamBaseResult.fail(CodeStatus.GENERAL_EXCEPTION_CODE, "can not find latest data");
        }

        CstStreamBaseResult<DeHourSource>  deFirstDaySources = (CstStreamBaseResult<DeHourSource>)cstStreamDayFirstIntegrateService
                .getDaySource(carId, deDayLatestData.getTime(), type, sourceColumns, sourceClazz);
        if (deFirstDaySources==null||deFirstDaySources.getData() == null) {
            return  CstStreamBaseResult.fail(CodeStatus.GENERAL_EXCEPTION_CODE, "can not find first data");
        }

        DeDayTransfor deDayTransfor = DeDayTransfor.builder()
                .rapidDecelerationCount(deDayLatestData.getRapidDecelerationCounts()-deFirstDaySources.getData().getRapidDecelerationCounts())
                .rapidAccelerationCount(deDayLatestData.getRapidAccelerationCounts()-deFirstDaySources.getData().getRapidAccelerationCounts())
                .sharpTurnCount(deDayLatestData.getSharpTurnCounts()-deFirstDaySources.getData().getSharpTurnCounts())
                .time(deDayLatestData.getTime())
                .carId(deDayLatestData.getCarId())
                .build();

        return CstStreamBaseResult.success(deDayTransfor);
    }




    public CstStreamBaseResult<GpsDayTransfor> getGpsNoDelayDayTransfor(String carId,String head) {
        GpsDayLatestData gpsDayLatestData;
        gpsDayLatestData= (GpsDayLatestData) redisCommonService.getLatestDataFromRedis(head, carId);
        if (gpsDayLatestData==null) {
            return  CstStreamBaseResult.fail(CodeStatus.GENERAL_EXCEPTION_CODE, "can not find latest data");
        }

        GpsDayTransfor gpsDayTransfor = GpsDayTransfor.builder()
                .gpsCount(gpsDayLatestData.getGpsCount())
                .isNonLocal(gpsDayLatestData.getIsNonLocal())
                .maxSatelliteNum(gpsDayLatestData.getMaxSatelliteNum())
                .time(gpsDayLatestData.getTime())
                .carId(gpsDayLatestData.getCarId())
                .build();

        return CstStreamBaseResult.success(gpsDayTransfor);

    }



    public CstStreamBaseResult<ObdDayTransfor> getObdNoDelayDayTransfor(String carId,String type,String head, String[] sourceColumns, Class<?> sourceClazz) {
        ObdDayLatestData obdDayLatestData;
        obdDayLatestData= (ObdDayLatestData) redisCommonService.getLatestDataFromRedis(head, carId);
        if (obdDayLatestData==null) {
            return  CstStreamBaseResult.fail(CodeStatus.GENERAL_EXCEPTION_CODE, "can not find latest data");
        }


        CstStreamBaseResult<ObdHourSource> obdFirstDaySources = (CstStreamBaseResult<ObdHourSource>)cstStreamDayFirstIntegrateService
                .getDaySource(carId, obdDayLatestData.getTime(), type, sourceColumns, sourceClazz);
        if (obdFirstDaySources == null||obdFirstDaySources.getData()==null) {
            return  CstStreamBaseResult.fail(CodeStatus.GENERAL_EXCEPTION_CODE, "can not find first data");
        }
        float mileage = (float) round(new BigDecimal(obdDayLatestData.getTotalDistance() - obdFirstDaySources.getData().getTotalDistance()), 3);
        int duration = obdDayLatestData.getRunTotalTime() - obdFirstDaySources.getData().getRunTotalTime();
        float fuel = (float) round(new BigDecimal(obdDayLatestData.getTotalFuel() - obdFirstDaySources.getData().getTotalFuel()), 3);


        ObdDayTransfor obdDayTransfor = ObdDayTransfor.builder()
                .din(obdDayLatestData.getDin())
                .duration(duration)
                .fuel(fuel)
                .isDrive(obdDayLatestData.getIsDrive())
                .isHighSpeed(obdDayLatestData.getIsHighSpeed())
                .isNightDrive(obdDayLatestData.getIsNightDrive())
                .maxSpeed(obdDayLatestData.getMaxSpeed())
                .mileage(mileage)
                .motormeterDistance(obdDayLatestData.getMotormeterDistance())
                .runTotalTime(obdDayLatestData.getRunTotalTime())
                .speed(obdDayLatestData.getSpeed())
                .totalDistance(obdDayLatestData.getTotalDistance())
                .totalFuel(obdDayLatestData.getTotalFuel())
                .fee((float)round(new BigDecimal(obdDayLatestData.getTotalFuel()-obdFirstDaySources.getData().getTotalFuel())
                        .multiply(new BigDecimal(redisCommonService.calcPrice(obdDayLatestData.getCarId(),obdDayLatestData.getTime()))),3))
                .fuelPerHundred(calcFuelPerHundred(fuel,mileage))
                .toolingProbability(obdDayLatestData.getToolingProbability())
                .dinChange(obdDayLatestData.getDinChange())
                .averageSpeed(calcAvarageSpeed(mileage,duration))
                .time(obdDayLatestData.getTime())
                .carId(obdDayLatestData.getCarId())

                .build();

        return CstStreamBaseResult.success(obdDayTransfor);
    }



    public CstStreamBaseResult<TraceDeleteDayTransfor> getTraceDeleteNoDelayDayTransfor(String carId,String head) {
        TraceDeleteDayLatestData traceDeleteDayLatestData;

        traceDeleteDayLatestData= (TraceDeleteDayLatestData) redisCommonService.getLatestDataFromRedis(head, carId);


        if (traceDeleteDayLatestData==null) {
            return  CstStreamBaseResult.fail(CodeStatus.GENERAL_EXCEPTION_CODE, "can not find latest data");
        }

        TraceDeleteDayTransfor traceDeleteDayTransfor= TraceDeleteDayTransfor.builder()
                .traceDeleteCounts(traceDeleteDayLatestData.getTraceDeleteCounts())
                .time(traceDeleteDayLatestData.getTime())
                .carId(traceDeleteDayLatestData.getCarId())
                .build();

        return CstStreamBaseResult.success(traceDeleteDayTransfor);
    }



    public CstStreamBaseResult<TraceDayTransfor> getTraceNoDelayDayTransfor(String carId,String head) {


        TraceDayLatestData traceDayLatestData;
        traceDayLatestData= (TraceDayLatestData) redisCommonService.getLatestDataFromRedis(head, carId);

        if (traceDayLatestData==null) {
            return  CstStreamBaseResult.fail(CodeStatus.GENERAL_EXCEPTION_CODE, "can not find latest data");
        }

        TraceDayTransfor traceDayTransfor= TraceDayTransfor.builder()
                .traceCounts(CollectionUtils.isNotEmpty(traceDayLatestData.getTraces())?traceDayLatestData.getTraces().size():0)
                .time(traceDayLatestData.getTime())
                .carId(traceDayLatestData.getCarId())
                .build();

        return CstStreamBaseResult.success(traceDayTransfor);
    }



    public CstStreamBaseResult<VoltageDayTransfor> getVoltageNoDelayDayTransfor(String carId,String head) {
        VoltageDayLatestData voltageDayLatestData;
        voltageDayLatestData= (VoltageDayLatestData) redisCommonService.getLatestDataFromRedis(head, carId);

        if (voltageDayLatestData==null) {
            return  CstStreamBaseResult.fail(CodeStatus.GENERAL_EXCEPTION_CODE, "can not find latest data");
        }
        VoltageDayTransfor voltageDayTransfor= VoltageDayTransfor.builder()
                .maxVoltage(voltageDayLatestData.getMaxVoltage())
                .minVoltage(voltageDayLatestData.getMinVoltage())
                .time(voltageDayLatestData.getTime())
                .carId(voltageDayLatestData.getCarId())
                .build();

        return CstStreamBaseResult.success(voltageDayTransfor);
    }


    public CstStreamBaseResult<DayIntegratedTransfor> getDayNoDelayDataDayTransfor(String carId) {
        long nowTime = System.currentTimeMillis();
        DayIntegratedTransfor dayIntegratedTransfor=DayIntegratedTransfor.builder()
                .carId(carId).time(nowTime)
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
                .isNightDrive(0)
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

        VoltageDayLatestData voltageDayLatestData;
        AmDayLatestData amDayLatestData;
        ObdDayLatestData obdDayLatestData;
        GpsDayLatestData gpsDayLatestData;
        DeDayLatestData deDayLatestData;
        TraceDayLatestData traceDayLatestData;
        TraceDeleteDayLatestData traceDeleteDayLatestData;
        MileageDayLatestData mileageDayLatestData;


        voltageDayLatestData= (VoltageDayLatestData) redisCommonService.getLatestDataFromRedis(StreamRedisConstants.DayKey.DAY_VOLTAGE, carId);
        amDayLatestData= (AmDayLatestData) redisCommonService.getLatestDataFromRedis(StreamRedisConstants.DayKey.DAY_AM, carId);
        obdDayLatestData= (ObdDayLatestData) redisCommonService.getLatestDataFromRedis(StreamRedisConstants.DayKey.DAY_OBD, carId);
        gpsDayLatestData= (GpsDayLatestData) redisCommonService.getLatestDataFromRedis(StreamRedisConstants.DayKey.DAY_GPS, carId);
        deDayLatestData= (DeDayLatestData) redisCommonService.getLatestDataFromRedis(StreamRedisConstants.DayKey.DAY_DE, carId);
        traceDayLatestData= (TraceDayLatestData) redisCommonService.getLatestDataFromRedis(StreamRedisConstants.DayKey.DAY_TRACE, carId);
        traceDeleteDayLatestData= (TraceDeleteDayLatestData) redisCommonService.getLatestDataFromRedis(StreamRedisConstants.DayKey.DAY_TRACE_DELETE, carId);
        mileageDayLatestData= (MileageDayLatestData) redisCommonService.getLatestDataFromRedis(StreamRedisConstants.DayKey.DAY_MILEAGE, carId);



        if (voltageDayLatestData != null
                && DateTimeUtil.dateTimeDifferentBetween(nowTime,voltageDayLatestData.getTime(),DateTimeUtil.DEFAULT_DATE_DAY)) {
            dayIntegratedTransfor.setMinVoltage(voltageDayLatestData.getMinVoltage());
            dayIntegratedTransfor.setMaxVoltage(voltageDayLatestData.getMaxVoltage());
            dayIntegratedTransfor.setTime(voltageDayLatestData.getTime());
        }
        if (amDayLatestData != null
                && DateTimeUtil.dateTimeDifferentBetween(nowTime,amDayLatestData.getTime(),DateTimeUtil.DEFAULT_DATE_DAY)) {
            dayIntegratedTransfor.setCollision(amDayLatestData.getCollision());
            dayIntegratedTransfor.setFlameOut(amDayLatestData.getFlameOut());
            dayIntegratedTransfor.setIgnition(amDayLatestData.getIgnition());
            dayIntegratedTransfor.setInsertNum(amDayLatestData.getInsertNum());
            dayIntegratedTransfor.setIsFatigue(amDayLatestData.getIsFatigue());
            dayIntegratedTransfor.setOverSpeed(amDayLatestData.getOverSpeed());
            dayIntegratedTransfor.setIsMissing(amDayLatestData.getIsMissing());
            dayIntegratedTransfor.setPulloutCounts(amDayLatestData.getPulloutCounts());
            dayIntegratedTransfor.setPulloutTimes(amDayLatestData.getPulloutTimes());
            if(amDayLatestData.getTime()!=nowTime && amDayLatestData.getTime()>dayIntegratedTransfor.getTime())
                dayIntegratedTransfor.setTime(amDayLatestData.getTime());
        }
        if(obdDayLatestData != null){
            dayIntegratedTransfor.setDin(obdDayLatestData.getDin());
            dayIntegratedTransfor.setMotormeterDistance(obdDayLatestData.getMotormeterDistance());
            dayIntegratedTransfor.setRunTotalTime(obdDayLatestData.getRunTotalTime());
            dayIntegratedTransfor.setTotalDistance(obdDayLatestData.getTotalDistance());
            dayIntegratedTransfor.setTotalFuel(obdDayLatestData.getTotalFuel());
        }
        if (obdDayLatestData != null
                && DateTimeUtil.dateTimeDifferentBetween(nowTime,obdDayLatestData.getTime(),DateTimeUtil.DEFAULT_DATE_DAY)) {
            ObdHourSource obdHourSource = (ObdHourSource)redisCommonService.getFirstDataFromRedis(StreamRedisConstants.DayKey.DAY_OBD, carId,
                    DateTimeUtil.toLongTimeString(obdDayLatestData.getTime(), DateTimeUtil.DEFAULT_DATE_DAY));
            if(obdHourSource==null) {
                CstStreamBaseResult<ObdHourSource> obdFirstHourSources = (CstStreamBaseResult<ObdHourSource>) cstStreamDayFirstIntegrateService
                        .getDaySource(carId, obdDayLatestData.getTime(), StreamTypeDefine.OBD_TYPE,
                                HbaseColumn.HourSourceColumn.obdHourColumns, ObdHourSource.class);
                if (obdFirstHourSources != null) {
                    obdHourSource = obdFirstHourSources.getData();
                }
            }
            if (obdHourSource!=null) {
                float mileage = (float) round(new BigDecimal(obdDayLatestData.getTotalDistance() - obdHourSource.getTotalDistance()), 3);
                dayIntegratedTransfor.setMileage(mileage);
                int duration = obdDayLatestData.getRunTotalTime() - obdHourSource.getRunTotalTime();
                dayIntegratedTransfor.setDuration(duration);
                float fuel = (float) round(new BigDecimal(obdDayLatestData.getTotalFuel() - obdHourSource.getTotalFuel()), 3);
                dayIntegratedTransfor.setFuel(fuel);
                dayIntegratedTransfor.setFee((float) round(new BigDecimal(fuel)
                        .multiply(new BigDecimal(redisCommonService.calcPrice(obdDayLatestData.getCarId(), obdDayLatestData.getTime()))), 3));
                dayIntegratedTransfor.setFuelPerHundred(calcFuelPerHundred(fuel,mileage));
                dayIntegratedTransfor.setAverageSpeed(calcAvarageSpeed(mileage, duration));
                if(obdDayLatestData.getTime()!=nowTime && obdDayLatestData.getTime()>dayIntegratedTransfor.getTime())
                    dayIntegratedTransfor.setTime(obdDayLatestData.getTime());
            }
            dayIntegratedTransfor.setIsDrive(obdDayLatestData.getIsDrive());
            dayIntegratedTransfor.setIsHighSpeed(obdDayLatestData.getIsHighSpeed());
            dayIntegratedTransfor.setIsNightDrive(obdDayLatestData.getIsNightDrive());
            dayIntegratedTransfor.setMaxSpeed(obdDayLatestData.getMaxSpeed());
            dayIntegratedTransfor.setSpeed(obdDayLatestData.getSpeed());
            dayIntegratedTransfor.setDinChange(obdDayLatestData.getDinChange());
            dayIntegratedTransfor.setToolingProbability(obdDayLatestData.getToolingProbability());
            dayIntegratedTransfor.setPowerConsumption(-1F);
        }

        if (gpsDayLatestData != null
                && DateTimeUtil.dateTimeDifferentBetween(nowTime,gpsDayLatestData.getTime(),DateTimeUtil.DEFAULT_DATE_DAY)) {
            dayIntegratedTransfor.setGpsCount(gpsDayLatestData.getGpsCount());
            dayIntegratedTransfor.setIsNonLocal(gpsDayLatestData.getIsNonLocal());
            dayIntegratedTransfor.setMaxSatelliteNum(gpsDayLatestData.getMaxSatelliteNum());
            if(gpsDayLatestData.getTime()!=nowTime && gpsDayLatestData.getTime()>dayIntegratedTransfor.getTime())
                dayIntegratedTransfor.setTime(gpsDayLatestData.getTime());
        }

        if (deDayLatestData != null
                && DateTimeUtil.dateTimeDifferentBetween(nowTime,deDayLatestData.getTime(),DateTimeUtil.DEFAULT_DATE_DAY)) {
            DeHourSource deHourSource = (DeHourSource)redisCommonService.getFirstDataFromRedis(StreamRedisConstants.DayKey.DAY_DE, carId,
                    DateTimeUtil.toLongTimeString(deDayLatestData.getTime(), DateTimeUtil.DEFAULT_DATE_DAY));
            if (deHourSource == null) {
                CstStreamBaseResult<DeHourSource>  deFirstHourSources = (CstStreamBaseResult<DeHourSource>)cstStreamDayFirstIntegrateService
                        .getDaySource(carId, deDayLatestData.getTime(),
                                StreamTypeDefine.DE_TYPE, HbaseColumn.HourSourceColumn.deHourColumns, DeHourSource.class);
                if (deFirstHourSources != null) {
                    deHourSource = deFirstHourSources.getData();
                }
            }

            if (deHourSource != null) {
                dayIntegratedTransfor.setRapidDecelerationCount(deDayLatestData.getRapidDecelerationCounts() - deHourSource.getRapidDecelerationCounts());
                dayIntegratedTransfor.setRapidAccelerationCount(deDayLatestData.getRapidAccelerationCounts() - deHourSource.getRapidAccelerationCounts());
                dayIntegratedTransfor.setSharpTurnCount(deDayLatestData.getSharpTurnCounts() - deHourSource.getSharpTurnCounts());
            }
            if(deDayLatestData.getTime()!=nowTime && deDayLatestData.getTime()>dayIntegratedTransfor.getTime())
                dayIntegratedTransfor.setTime(deDayLatestData.getTime());
        }
        if (traceDayLatestData != null
                && DateTimeUtil.dateTimeDifferentBetween(nowTime,traceDayLatestData.getTime(),DateTimeUtil.DEFAULT_DATE_DAY)) {
            dayIntegratedTransfor.setTraceCounts(CollectionUtils.isNotEmpty(traceDayLatestData.getTraces())?traceDayLatestData.getTraces().size():0);
            if(traceDayLatestData.getTime()!=nowTime && traceDayLatestData.getTime()>dayIntegratedTransfor.getTime())
                dayIntegratedTransfor.setTime(traceDayLatestData.getTime());
        }
        if (traceDeleteDayLatestData != null
                && DateTimeUtil.dateTimeDifferentBetween(nowTime,traceDeleteDayLatestData.getTime(),DateTimeUtil.DEFAULT_DATE_DAY)) {
            dayIntegratedTransfor.setTraceDeleteCounts(traceDeleteDayLatestData.getTraceDeleteCounts());
            if(traceDeleteDayLatestData.getTime()!=nowTime && traceDeleteDayLatestData.getTime()>dayIntegratedTransfor.getTime())
                dayIntegratedTransfor.setTime(traceDeleteDayLatestData.getTime());
        }

        if (mileageDayLatestData != null) {
            dayIntegratedTransfor.setMilObdTotalDistance(mileageDayLatestData.getMilObdTotalDistance());
            dayIntegratedTransfor.setMilGpsTotalDistance(mileageDayLatestData.getMilGpsTotalDistance());
            dayIntegratedTransfor.setMilRunTotalTime(mileageDayLatestData.getMilRunTotalTime());
            dayIntegratedTransfor.setMilTotalFuel(mileageDayLatestData.getMilTotalFuel());
            dayIntegratedTransfor.setPanelDistance(mileageDayLatestData.getPanelDistance());
            dayIntegratedTransfor.setGpsSpeed(mileageDayLatestData.getGpsSpeed());
            dayIntegratedTransfor.setObdSpeed(mileageDayLatestData.getObdSpeed());
        }

        if (mileageDayLatestData != null
                && DateTimeUtil.dateTimeDifferentBetween(nowTime,mileageDayLatestData.getTime(),DateTimeUtil.DEFAULT_DATE_DAY)) {
            MileageHourSource mileageHourSource = (MileageHourSource)redisCommonService.getFirstDataFromRedis(StreamRedisConstants.DayKey.DAY_MILEAGE, carId,
                    DateTimeUtil.toLongTimeString(mileageDayLatestData.getTime(), DateTimeUtil.DEFAULT_DATE_DAY));
            if (mileageHourSource == null) {
                CstStreamBaseResult<MileageHourSource>  mileageFirstHourSources = (CstStreamBaseResult<MileageHourSource>)cstStreamDayFirstIntegrateService
                        .getDaySource(carId, deDayLatestData.getTime(),
                                StreamTypeDefine.MILEAGE_TYPE, HbaseColumn.HourSourceColumn.mileageHourColumns, MileageHourSource.class);
                if (mileageFirstHourSources != null) {
                    mileageHourSource = mileageFirstHourSources.getData();
                }
            }

            if (mileageHourSource != null) {
                double fuel =   round(new BigDecimal(mileageDayLatestData.getMilTotalFuel() - mileageHourSource.getMilTotalFuel()), 3);
                double fee =  round(new BigDecimal(fuel)
                        .multiply(new BigDecimal(redisCommonService.calcPrice(mileageDayLatestData.getCarId(), mileageHourSource.getTime()))), 3);
                double gpsMil= round(BigDecimal.valueOf(mileageDayLatestData.getMilGpsTotalDistance())
                        .subtract(BigDecimal.valueOf(mileageHourSource.getMilGpsTotalDistance())), 3);
                double obdMil=  round(BigDecimal.valueOf(mileageDayLatestData.getMilObdTotalDistance())
                        .subtract(BigDecimal.valueOf(mileageHourSource.getMilObdTotalDistance())), 3);
                double panelMil=  round(BigDecimal.valueOf(mileageDayLatestData.getPanelDistance())
                        .subtract(BigDecimal.valueOf(mileageHourSource.getPanelDistance())), 3);
                dayIntegratedTransfor.setMilGpsMaxSpeed((float)Math.max(mileageDayLatestData.getGpsSpeed(),mileageHourSource.getGpsSpeed()));
                dayIntegratedTransfor.setMilObdMaxSpeed((float) Math.max(mileageDayLatestData.getObdSpeed(), mileageHourSource.getObdSpeed()));
                dayIntegratedTransfor.setMilObdMileage(obdMil);
                dayIntegratedTransfor.setMilGpsMileage(gpsMil);
                dayIntegratedTransfor.setMilPanelMileage(panelMil);
                dayIntegratedTransfor.setMilFee(fee);
                dayIntegratedTransfor.setMilDuration((int)(mileageDayLatestData.getMilRunTotalTime()-mileageHourSource.getMilRunTotalTime()));
            }
            if(mileageHourSource.getTime()!=nowTime && mileageHourSource.getTime()>dayIntegratedTransfor.getTime())
                dayIntegratedTransfor.setTime(mileageHourSource.getTime());
        }
        return CstStreamBaseResult.success(dayIntegratedTransfor);
    }



}

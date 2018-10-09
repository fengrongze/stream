package com.cst.bigdata.service.integrate;

import com.cst.bigdata.domain.mybatis.validatedata.IntegrateDayVo;
import com.cst.bigdata.domain.mybatis.validatedata.IntegrateHourVo;
import com.cst.bigdata.service.dubbo.CarInfoService;
import com.cst.bigdata.service.hbase.CstDataFromBigDaoService;
import com.cst.gdcp.di.DataPackage;
import com.cst.gdcp.protobuf.common.CSTAlarmTypeConst;
import com.cst.hfrq.dto.car.CarInfo;
import com.cst.stream.common.DateTimeUtils;
import com.cst.stream.stathour.am.AmDayTransfor;
import com.cst.stream.stathour.am.AmHourTransfor;
import com.cst.stream.stathour.de.DeDayTransfor;
import com.cst.stream.stathour.de.DeHourTransfor;
import com.cst.stream.stathour.gps.GpsDayTransfor;
import com.cst.stream.stathour.gps.GpsHourTransfor;
import com.cst.stream.stathour.obd.ObdDayTransfor;
import com.cst.stream.stathour.obd.ObdHourTransfor;
import com.cst.stream.stathour.trace.TraceDayTransfor;
import com.cst.stream.stathour.trace.TraceHourTransfor;
import com.cst.stream.stathour.tracedelete.TraceDeleteDayTransfor;
import com.cst.stream.stathour.tracedelete.TraceDeleteHourTransfor;
import com.cst.stream.stathour.voltage.VoltageDayTransfor;
import com.cst.stream.stathour.voltage.VoltageHourTransfor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Date;
import java.util.List;

import static com.cst.stream.common.DateTimeUtils.convertDateFromStr;
import static com.cst.stream.common.DateTimeUtils.convertDateoLDTT;
import static com.cst.stream.common.DateTimeUtils.convertLDTToDate;


/**
 * @author Johnney.Chiu
 * create on 2018/6/22 15:45
 * @Description 验证数据
 * @title
 */

@Service
@Slf4j
public class CstStramDataValidateIntegratedService {

    @Autowired
    private CstDataFromBigDaoService cstDataFromBigDaoService;

    @Autowired
    private CarInfoService carInfoService;




    public ObdHourTransfor getHourObdDataAnalysisFromBigDao(String din, String dateStrFrom, String dateStrTo){
        ObdHourTransfor obdHourTransfor =ObdHourTransfor.builder()
                .maxSpeed(0f)
                .fuel(0f)
                .fee(0f)
                .mileage(0f)
                .duration(0)
                .isDrive(0)
                .isHighSpeed(0)
                .build();


        Date fromDate= convertDateFromStr(dateStrFrom, DateTimeUtils.TimeFormat.LONG_DATE_PATTERN_LINE);
        Date toDate= convertDateFromStr(dateStrTo, DateTimeUtils.TimeFormat.LONG_DATE_PATTERN_LINE);


        List<DataPackage> fromZoneList = cstDataFromBigDaoService.getObdData(din, fromDate, toDate);
        if (CollectionUtils.isEmpty(fromZoneList)) {
            return obdHourTransfor;
        }
        log.debug("find zone data from din {},time from {},time to{},data is {}",din,fromDate,toDate,fromZoneList);
        //找到下一个小时的数据
        //小时加一
        int i = 0;
        List<DataPackage> secondZoneList = null;
        while (CollectionUtils.isEmpty(secondZoneList)){
            fromDate = convertLDTToDate(convertDateoLDTT(fromDate).plusHours(1));
            toDate=convertLDTToDate(convertDateoLDTT(toDate).plusHours(1));
            secondZoneList = cstDataFromBigDaoService.getObdData(din,fromDate,toDate);
            if (++i == 50) {
                break;
            }
        }
        if (CollectionUtils.isEmpty(secondZoneList)) {
            return obdHourTransfor;
        }
        log.debug("find second data from din {},time from {},time to{},data is {}",din,fromDate,toDate,fromZoneList);

        int hour=Integer.parseInt(dateStrFrom.substring(11,13));

        int maxSpeed=fromZoneList.parallelStream().filter(d -> d.obd != null)
                .map(d -> d.obd.carSpeed).reduce(Integer::max).orElse(0);
        int secondSpeed = secondZoneList.parallelStream().filter(d -> d.obd != null)
                .findFirst().map(d -> d.obd.carSpeed).orElse(0);

        float firstFuel=fromZoneList.parallelStream().filter(d -> d.accumulated != null&&d.accumulated.totalFuel!=null&&d.accumulated.totalFuel>=0)
                .findFirst().map(d -> d.accumulated.totalFuel).orElse(0f);
        float secondFuel=secondZoneList.parallelStream().filter(d -> d.accumulated != null&&d.accumulated.totalFuel!=null&&d.accumulated.totalFuel>=0)
                .findFirst().map(d -> d.accumulated.totalFuel).orElse(0f);

        float fuel = (float) round(new BigDecimal(secondFuel).subtract(new BigDecimal(firstFuel)), 3);

        float fee = (float) round(new BigDecimal(7.8f).multiply(new BigDecimal(fuel)), 3);

        float firstMileage=fromZoneList.parallelStream().filter(d -> d.accumulated != null&&d.accumulated.totalDistance!=null&&d.accumulated.totalDistance>=0)
                .findFirst().map(d -> d.accumulated.totalDistance).orElse(0f);
        float secondMileage=secondZoneList.parallelStream().filter(d -> d.accumulated != null&&d.accumulated.totalDistance!=null&&d.accumulated.totalDistance>=0)
                .findFirst().map(d -> d.accumulated.totalDistance).orElse(0f);

        float mileage = (float) round(new BigDecimal(secondMileage).subtract(new BigDecimal(firstMileage)), 3);

        int firstRunTime=fromZoneList.parallelStream().filter(d -> d.accumulated != null&&d.accumulated.runTotalTime!=null&&d.accumulated.runTotalTime>=0)
                .findFirst().map(d -> d.accumulated.runTotalTime).orElse(0);
        int secondRunTime=secondZoneList.parallelStream().filter(d -> d.accumulated != null&&d.accumulated.runTotalTime!=null&&d.accumulated.runTotalTime>=0)
                .findFirst().map(d -> d.accumulated.runTotalTime).orElse(0);
        int runTime = secondRunTime - firstRunTime;


        obdHourTransfor.setMaxSpeed((float)Math.max(maxSpeed,secondSpeed));
        obdHourTransfor.setFuel(fuel);
        obdHourTransfor.setFee(fee);
        obdHourTransfor.setMileage(mileage);
        obdHourTransfor.setDuration(runTime);
        obdHourTransfor.setIsDrive((runTime>0||maxSpeed>0)?1:0);
        obdHourTransfor.setIsHighSpeed(maxSpeed>100?1:0);


        log.debug("calc data is {}",obdHourTransfor);
        return obdHourTransfor;
    }
    public ObdDayTransfor getDayObdDataAnalysisFromBigDao(String din, String dateStrFrom, String dateStrTo){
        ObdDayTransfor obdDayTransfor =ObdDayTransfor.builder()
                .maxSpeed(0f)
                .fuel(0f)
                .fee(0f)
                .mileage(0f)
                .duration(0)
                .isDrive(0)
                .isHighSpeed(0)
                .isNightDrive(0)
                .build();


        Date fromDate= convertDateFromStr(dateStrFrom, DateTimeUtils.TimeFormat.LONG_DATE_PATTERN_LINE);
        Date toDate= convertDateFromStr(dateStrTo, DateTimeUtils.TimeFormat.LONG_DATE_PATTERN_LINE);


        List<DataPackage> fromZoneList = cstDataFromBigDaoService.getObdData(din, fromDate, toDate);
        if (CollectionUtils.isEmpty(fromZoneList)) {
            return obdDayTransfor;
        }
        log.debug("find zone data from din {},time from {},time to{},data is {}",din,fromDate,toDate,fromZoneList);
        //找到下一个小时的数据
        //小时加一
        int i = 0;
        List<DataPackage> secondZoneList = null;
        while (CollectionUtils.isEmpty(secondZoneList)){
            fromDate = convertLDTToDate(convertDateoLDTT(fromDate).plusDays(1));
            toDate=convertLDTToDate(convertDateoLDTT(toDate).plusDays(1));
            secondZoneList = cstDataFromBigDaoService.getObdData(din,fromDate,toDate);
            if (++i == 50) {
                break;
            }
        }
        if (CollectionUtils.isEmpty(secondZoneList)) {
            return obdDayTransfor;
        }
        log.debug("find second data from din {},time from {},time to{},data is {}",din,fromDate,toDate,fromZoneList);

        int hour=Integer.parseInt(dateStrFrom.substring(11,13));

        int maxSpeed=fromZoneList.parallelStream().filter(d -> d.obd != null)
                .map(d -> d.obd.carSpeed).reduce(Integer::max).orElse(0);
        int secondSpeed = secondZoneList.parallelStream().filter(d -> d.obd != null)
                .findFirst().map(d -> d.obd.carSpeed).orElse(0);

        float firstFuel=fromZoneList.parallelStream().filter(d -> d.accumulated != null&&d.accumulated.totalFuel!=null&&d.accumulated.totalFuel>=0)
                .findFirst().map(d -> d.accumulated.totalFuel).orElse(0f);
        float secondFuel=secondZoneList.parallelStream().filter(d -> d.accumulated != null&&d.accumulated.totalFuel!=null&&d.accumulated.totalFuel>=0)
                .findFirst().map(d -> d.accumulated.totalFuel).orElse(0f);

        float fuel = (float) round(new BigDecimal(secondFuel).subtract(new BigDecimal(firstFuel)), 3);

        float fee = (float) round(new BigDecimal(7.8f).multiply(new BigDecimal(fuel)), 3);

        float firstMileage=fromZoneList.parallelStream().filter(d -> d.accumulated != null&&d.accumulated.totalDistance!=null&&d.accumulated.totalDistance>=0)
                .findFirst().map(d -> d.accumulated.totalDistance).orElse(0f);
        float secondMileage=secondZoneList.parallelStream().filter(d -> d.accumulated != null&&d.accumulated.totalDistance!=null&&d.accumulated.totalDistance>=0)
                .findFirst().map(d -> d.accumulated.totalDistance).orElse(0f);

        float mileage = (float) round(new BigDecimal(secondMileage).subtract(new BigDecimal(firstMileage)), 3);

        int firstRunTime=fromZoneList.parallelStream().filter(d -> d.accumulated != null&&d.accumulated.runTotalTime!=null&&d.accumulated.runTotalTime>=0)
                .findFirst().map(d -> d.accumulated.runTotalTime).orElse(0);
        int secondRunTime=secondZoneList.parallelStream().filter(d -> d.accumulated != null&&d.accumulated.runTotalTime!=null&&d.accumulated.runTotalTime>=0)
                .findFirst().map(d -> d.accumulated.runTotalTime).orElse(0);
        int runTime = secondRunTime - firstRunTime;


        obdDayTransfor.setMaxSpeed((float)Math.max(maxSpeed,secondSpeed));
        obdDayTransfor.setFuel(fuel);
        obdDayTransfor.setFee(fee);
        obdDayTransfor.setMileage(mileage);
        obdDayTransfor.setDuration(runTime);
        obdDayTransfor.setIsDrive((runTime>0||maxSpeed>0)?1:0);
        obdDayTransfor.setIsHighSpeed(maxSpeed>100?1:0);
        obdDayTransfor.setIsNightDrive((hour>=23||hour<6)?1:0);


        log.debug("calc data is {}",obdDayTransfor);
        return obdDayTransfor;
    }


    public GpsHourTransfor getHourGpsDataAnalysisFromBigDao(String din, String dateStrFrom, String dateStrTo){
        GpsHourTransfor gpsHourTransfor = new GpsHourTransfor()
                .builder()
                .maxSatelliteNum(0)
                .isNonLocal(0)
                .gpsCount(0)
                .build();

        Date fromDate= convertDateFromStr(dateStrFrom, DateTimeUtils.TimeFormat.LONG_DATE_PATTERN_LINE);
        Date toDate= convertDateFromStr(dateStrTo, DateTimeUtils.TimeFormat.LONG_DATE_PATTERN_LINE);


        List<DataPackage> fromZoneList = cstDataFromBigDaoService.getGpsData(din, fromDate, toDate);
        if (CollectionUtils.isEmpty(fromZoneList)) {
            return gpsHourTransfor;
        }
        int maxSa = fromZoneList.parallelStream().filter(d -> d.gps != null &&d.gps.satellites !=null)
                .map(d -> d.gps.satellites).reduce(Integer::max).orElse(0);
        int gpsCount = (int)(fromZoneList.parallelStream().filter(d -> d.gps != null).count());

        gpsHourTransfor.setGpsCount(gpsCount);
        gpsHourTransfor.setMaxSatelliteNum(maxSa);

        return gpsHourTransfor;
    }

    public GpsDayTransfor getDayGpsDataAnalysisFromBigDao(String din, String dateStrFrom, String dateStrTo){
        GpsDayTransfor gpsDayTransfor = new GpsDayTransfor()
                .builder()
                .maxSatelliteNum(0)
                .isNonLocal(0)
                .gpsCount(0)
                .build();

        Date fromDate= convertDateFromStr(dateStrFrom, DateTimeUtils.TimeFormat.LONG_DATE_PATTERN_LINE);
        Date toDate= convertDateFromStr(dateStrTo, DateTimeUtils.TimeFormat.LONG_DATE_PATTERN_LINE);


        List<DataPackage> fromZoneList = cstDataFromBigDaoService.getGpsData(din, fromDate, toDate);
        if (CollectionUtils.isEmpty(fromZoneList)) {
            return gpsDayTransfor;
        }
        int maxSa = fromZoneList.parallelStream().filter(d -> d.gps != null &&d.gps.satellites !=null)
                .map(d -> d.gps.satellites).reduce(Integer::max).orElse(0);
        int gpsCount = (int)(fromZoneList.parallelStream().filter(d -> d.gps != null).count());

        gpsDayTransfor.setGpsCount(gpsCount);
        gpsDayTransfor.setMaxSatelliteNum(maxSa);

        return gpsDayTransfor;
    }

    public TraceHourTransfor getHourTraceDataAnalysisFromBigDao(String din, String dateStrFrom, String dateStrTo){
        TraceHourTransfor traceHourTransfor = new TraceHourTransfor()
                .builder()
               .traceCounts(0)
                .build();

        Date fromDate= convertDateFromStr(dateStrFrom, DateTimeUtils.TimeFormat.LONG_DATE_PATTERN_LINE);
        Date toDate= convertDateFromStr(dateStrTo, DateTimeUtils.TimeFormat.LONG_DATE_PATTERN_LINE);


        List<DataPackage> fromZoneList = cstDataFromBigDaoService.getTraceData(din, fromDate, toDate);
        if (CollectionUtils.isEmpty(fromZoneList)) {
            return traceHourTransfor;
        }
        int traceCounts=(int)(fromZoneList.parallelStream().filter(d -> d.travel != null).count());

        traceHourTransfor.setTraceCounts(traceCounts);

        return traceHourTransfor;
    }
    public TraceDayTransfor getDayTraceDataAnalysisFromBigDao(String din, String dateStrFrom, String dateStrTo){
        TraceDayTransfor traceDayTransfor = TraceDayTransfor
                .builder()
                .traceCounts(0)
                .build();

        Date fromDate= convertDateFromStr(dateStrFrom, DateTimeUtils.TimeFormat.LONG_DATE_PATTERN_LINE);
        Date toDate= convertDateFromStr(dateStrTo, DateTimeUtils.TimeFormat.LONG_DATE_PATTERN_LINE);


        List<DataPackage> fromZoneList = cstDataFromBigDaoService.getTraceData(din, fromDate, toDate);
        if (CollectionUtils.isEmpty(fromZoneList)) {
            return traceDayTransfor;
        }
        int traceCounts=(int)(fromZoneList.parallelStream().filter(d -> d.travel != null).count());

        traceDayTransfor.setTraceCounts(traceCounts);

        return traceDayTransfor;
    }


    public VoltageHourTransfor getHourVoltageDataAnalysisFromBigDao(String din, String dateStrFrom, String dateStrTo){
        VoltageHourTransfor voltageTransfor = new VoltageHourTransfor()
                .builder()
                .maxVoltage(0f)
                .minVoltage(0f)
                .build();

        Date fromDate= convertDateFromStr(dateStrFrom, DateTimeUtils.TimeFormat.LONG_DATE_PATTERN_LINE);
        Date toDate= convertDateFromStr(dateStrTo, DateTimeUtils.TimeFormat.LONG_DATE_PATTERN_LINE);


        List<DataPackage> fromZoneList = cstDataFromBigDaoService.getVoltageData(din, fromDate, toDate);
        if (CollectionUtils.isEmpty(fromZoneList)) {
            return voltageTransfor;
        }
        float maxVoltage = fromZoneList.parallelStream().filter(d -> d.voltageGather != null
                && CollectionUtils.isNotEmpty(d.voltageGather.voltageData)).map(d -> d.voltageGather)
                .flatMap(v -> v.voltageData.parallelStream())
                .map(x -> x.voltage).reduce(Float::max).orElse(0f);

        float minVoltage = fromZoneList.parallelStream().filter(d -> d.voltageGather != null
                && CollectionUtils.isNotEmpty(d.voltageGather.voltageData)).map(d -> d.voltageGather)
                .flatMap(v -> v.voltageData.parallelStream())
                .map(x -> x.voltage).reduce(Float::min).orElse(0f);

        voltageTransfor.setMaxVoltage(maxVoltage);
        voltageTransfor.setMinVoltage(minVoltage);

        return voltageTransfor;
    }
    public VoltageDayTransfor getDayVoltageDataAnalysisFromBigDao(String din, String dateStrFrom, String dateStrTo){
        VoltageDayTransfor voltageDayTransfor = VoltageDayTransfor
                .builder()
                .maxVoltage(0f)
                .minVoltage(0f)
                .build();

        Date fromDate= convertDateFromStr(dateStrFrom, DateTimeUtils.TimeFormat.LONG_DATE_PATTERN_LINE);
        Date toDate= convertDateFromStr(dateStrTo, DateTimeUtils.TimeFormat.LONG_DATE_PATTERN_LINE);


        List<DataPackage> fromZoneList = cstDataFromBigDaoService.getVoltageData(din, fromDate, toDate);
        if (CollectionUtils.isEmpty(fromZoneList)) {
            return voltageDayTransfor;
        }
        float maxVoltage = fromZoneList.parallelStream().filter(d -> d.voltageGather != null
                && CollectionUtils.isNotEmpty(d.voltageGather.voltageData)).map(d -> d.voltageGather)
                .flatMap(v -> v.voltageData.parallelStream())
                .map(x -> x.voltage).reduce(Float::max).orElse(0f);

        float minVoltage = fromZoneList.parallelStream().filter(d -> d.voltageGather != null
                && CollectionUtils.isNotEmpty(d.voltageGather.voltageData)).map(d -> d.voltageGather)
                .flatMap(v -> v.voltageData.parallelStream())
                .map(x -> x.voltage).reduce(Float::min).orElse(0f);

        voltageDayTransfor.setMaxVoltage(maxVoltage);
        voltageDayTransfor.setMinVoltage(minVoltage);

        return voltageDayTransfor;
    }

    public AmHourTransfor getHourAmDataAnalysisFromBigDao(String din, String dateStrFrom, String dateStrTo){
        AmHourTransfor amHourTransfor = new AmHourTransfor()
                .builder()
                .isMissing(0)
                .overSpeed(0)
                .isFatigue(0)
                .insertNum(0)
                .ignition(0)
                .flameOut(0)
                .collision(0)
                .pulloutTimes(0f)
                .pulloutCounts(0)
                .build();

        Date fromDate= convertDateFromStr(dateStrFrom, DateTimeUtils.TimeFormat.LONG_DATE_PATTERN_LINE);
        Date toDate= convertDateFromStr(dateStrTo, DateTimeUtils.TimeFormat.LONG_DATE_PATTERN_LINE);


        List<DataPackage> fromZoneList = cstDataFromBigDaoService.getAlarmData(din, fromDate, toDate);
        if (CollectionUtils.isEmpty(fromZoneList)) {
            return amHourTransfor;
        }

        int collision=(int)(fromZoneList.parallelStream().filter(d -> d.alarm != null && d.alarm.alarmType!=null&& (CSTAlarmTypeConst.ALARM_COLLISION == d.alarm.alarmType
                && d.alarm.collisionGatherType != 3)).count());
        int flameout = (int) (fromZoneList.parallelStream().filter(d -> d.alarm != null && d.alarm.alarmType!=null&& (CSTAlarmTypeConst.ALARM_CAR_FLAMEOUT == d.alarm.alarmType))
                .count());
        int ignition = (int) (fromZoneList.parallelStream().filter(d -> d.alarm != null && d.alarm.alarmType!=null&& (CSTAlarmTypeConst.ALARM_CAR_IGNITION == d.alarm.alarmType))
                .count());
        int inesert = (int) (fromZoneList.parallelStream().filter(d -> d.alarm != null && d.alarm.alarmType!=null&& (CSTAlarmTypeConst.ALARM_TERMINAL_INSERT == d.alarm.alarmType))
                .count());
        int isfatigue=fromZoneList.parallelStream().filter(d -> d.alarm != null && d.alarm.alarmType!=null&& (CSTAlarmTypeConst.ALARM_FATIGUE_DRIVING_PARA == d.alarm.alarmType))
                .findAny().isPresent()?1:0;
        int disappear=fromZoneList.parallelStream().filter(d -> d.alarm != null && d.alarm.alarmType!=null&& (CSTAlarmTypeConst.ALARM_DISAPPEAR_STATE == d.alarm.alarmType))
                .findAny().isPresent()?1:0;
        int overSpeed=(int) (fromZoneList.parallelStream().filter(d -> d.alarm != null && d.alarm.alarmType!=null&& (CSTAlarmTypeConst.ALARM_OVERSPEED == d.alarm.alarmType))
                .count());
        float pulloutTimes=fromZoneList.parallelStream().filter(d -> d.alarm != null && d.alarm.alarmType!=null&& (CSTAlarmTypeConst.ALARM_OVERSPEED == d.alarm.alarmType)
                && StringUtils.isNotEmpty(d.alarm.troubleCode)).map(d-> NumberUtils.toFloat(d.alarm.troubleCode,0f))
                .reduce(Float::max).orElse(0f);
        int pulloutCounts=(int) (fromZoneList.parallelStream().filter(d -> d.alarm != null && d.alarm.alarmType!=null&& (CSTAlarmTypeConst.ALARM_LONG_NOGPSINF_TIME == d.alarm.alarmType)
        ).count());

        amHourTransfor.setIsFatigue(isfatigue);
        amHourTransfor.setIsMissing(disappear);
        amHourTransfor.setOverSpeed(overSpeed);
        amHourTransfor.setCollision(collision);
        amHourTransfor.setInsertNum(inesert);
        amHourTransfor.setFlameOut(flameout);
        amHourTransfor.setIgnition(ignition);
        amHourTransfor.setPulloutTimes(pulloutTimes);
        amHourTransfor.setPulloutCounts(pulloutCounts);


        return amHourTransfor;
    }
    public AmDayTransfor getDayAmDataAnalysisFromBigDao(String din, String dateStrFrom, String dateStrTo){
        AmDayTransfor amDayTransfor = AmDayTransfor
                .builder()
                .isMissing(0)
                .overSpeed(0)
                .isFatigue(0)
                .insertNum(0)
                .ignition(0)
                .flameOut(0)
                .collision(0)
                .pulloutTimes(0f)
                .pulloutCounts(0)
                .build();

        Date fromDate= convertDateFromStr(dateStrFrom, DateTimeUtils.TimeFormat.LONG_DATE_PATTERN_LINE);
        Date toDate= convertDateFromStr(dateStrTo, DateTimeUtils.TimeFormat.LONG_DATE_PATTERN_LINE);


        List<DataPackage> fromZoneList = cstDataFromBigDaoService.getAlarmData(din, fromDate, toDate);
        if (CollectionUtils.isEmpty(fromZoneList)) {
            return amDayTransfor;
        }

        int collision=(int)(fromZoneList.parallelStream().filter(d -> d.alarm != null && d.alarm.alarmType!=null&& (CSTAlarmTypeConst.ALARM_COLLISION == d.alarm.alarmType
                && d.alarm.collisionGatherType != 3)).count());
        int flameout = (int) (fromZoneList.parallelStream().filter(d -> d.alarm != null && d.alarm.alarmType!=null&& (CSTAlarmTypeConst.ALARM_CAR_FLAMEOUT == d.alarm.alarmType))
                .count());
        int ignition = (int) (fromZoneList.parallelStream().filter(d -> d.alarm != null && d.alarm.alarmType!=null&& (CSTAlarmTypeConst.ALARM_CAR_IGNITION == d.alarm.alarmType))
                .count());
        int inesert = (int) (fromZoneList.parallelStream().filter(d -> d.alarm != null && d.alarm.alarmType!=null&& (CSTAlarmTypeConst.ALARM_TERMINAL_INSERT == d.alarm.alarmType))
                .count());
        int isfatigue=fromZoneList.parallelStream().filter(d -> d.alarm != null && d.alarm.alarmType!=null&& (CSTAlarmTypeConst.ALARM_FATIGUE_DRIVING_PARA == d.alarm.alarmType))
                .findAny().isPresent()?1:0;
        int disappear=fromZoneList.parallelStream().filter(d -> d.alarm != null && d.alarm.alarmType!=null&& (CSTAlarmTypeConst.ALARM_DISAPPEAR_STATE == d.alarm.alarmType))
                .findAny().isPresent()?1:0;
        int overSpeed = (int)fromZoneList.parallelStream().filter(d -> d.alarm != null && d.alarm.alarmType != null && (CSTAlarmTypeConst.ALARM_OVERSPEED == d.alarm.alarmType)).count();
        float pulloutTimes=fromZoneList.parallelStream().filter(d -> d.alarm != null && d.alarm.alarmType!=null&& (CSTAlarmTypeConst.ALARM_OVERSPEED == d.alarm.alarmType)
                && StringUtils.isNotEmpty(d.alarm.troubleCode)).map(d-> NumberUtils.toFloat(d.alarm.troubleCode,0f))
                .reduce(Float::max).orElse(0f);
        int pullout=(int) (fromZoneList.parallelStream().filter(d -> d.alarm != null && d.alarm.alarmType!=null&& (CSTAlarmTypeConst.ALARM_LONG_NOGPSINF_TIME == d.alarm.alarmType)
            ).count());
        amDayTransfor.setIsFatigue(isfatigue);
        amDayTransfor.setIsMissing(disappear);
        amDayTransfor.setOverSpeed(overSpeed);
        amDayTransfor.setCollision(collision);
        amDayTransfor.setInsertNum(inesert);
        amDayTransfor.setFlameOut(flameout);
        amDayTransfor.setIgnition(ignition);
        amDayTransfor.setPulloutTimes(pulloutTimes);
        amDayTransfor.setPulloutCounts(pullout);


        return amDayTransfor;
    }

    public TraceDeleteHourTransfor getHourTraceDeleteDataAnalysisFromBigDao(String din, String dateStrFrom, String dateStrTo){
        TraceDeleteHourTransfor traceDeleteHourTransfor = new TraceDeleteHourTransfor()
                .builder()
                .traceDeleteCounts(0)
                .build();
        return traceDeleteHourTransfor;

    }

    public TraceDeleteDayTransfor getDayTraceDeleteDataAnalysisFromBigDao(String din, String dateStrFrom, String dateStrTo){
        TraceDeleteDayTransfor traceDeleteDayTransfor = TraceDeleteDayTransfor
                .builder()
                .traceDeleteCounts(0)
                .build();
        return traceDeleteDayTransfor;

    }

    public DeHourTransfor getHourDeDataAnalysisFromBigDao(String din, String dateStrFrom, String dateStrTo){
        DeHourTransfor deHourTransfor =new DeHourTransfor().builder()
                .sharpTurnCount(0)
                .rapidAccelerationCount(0)
                .rapidDecelerationCount(0)
                .build();


        Date fromDate= convertDateFromStr(dateStrFrom, DateTimeUtils.TimeFormat.LONG_DATE_PATTERN_LINE);
        Date toDate= convertDateFromStr(dateStrTo, DateTimeUtils.TimeFormat.LONG_DATE_PATTERN_LINE);


        List<DataPackage> fromZoneList = cstDataFromBigDaoService.getDriveEventData(din, fromDate, toDate);
        if (CollectionUtils.isEmpty(fromZoneList)) {
            return deHourTransfor;
        }
        log.debug("find zone data from din {},time from {},time to{},data is {}",din,fromDate,toDate,fromZoneList);
        //找到下一个小时的数据
        //小时加一
        int i = 0;
        List<DataPackage> secondZoneList = null;
        while (CollectionUtils.isEmpty(secondZoneList)){
            fromDate = convertLDTToDate(convertDateoLDTT(fromDate).plusHours(1));
            toDate=convertLDTToDate(convertDateoLDTT(toDate).plusHours(1));
            secondZoneList = cstDataFromBigDaoService.getDriveEventData(din,fromDate,toDate);
            if (++i == 50) {
                break;
            }
        }
        if (CollectionUtils.isEmpty(secondZoneList)) {
            return deHourTransfor;
        }
        log.debug("find second data from din {},time from {},time to{},data is {}",din,fromDate,toDate,fromZoneList);
        int firstST=fromZoneList.parallelStream().filter(d ->d.accumulated!= null&&d.accumulated.sharpTurnTotalTimes!=null&&d.accumulated.sharpTurnTotalTimes>=0)
                .findFirst().map(d -> d.accumulated.sharpTurnTotalTimes).orElse(0);
        int secondST=secondZoneList.parallelStream().filter(d -> d.accumulated != null&&d.accumulated.sharpTurnTotalTimes!=null&&d.accumulated.sharpTurnTotalTimes>=0)
                .findFirst().map(d -> d.accumulated.sharpTurnTotalTimes).orElse(0);
        int sharpTurnTotalTimes = secondST - firstST;

        int firstAC=fromZoneList.parallelStream().filter(d ->d.accumulated!= null&&d.accumulated.accTotalTimes!=null&&d.accumulated.accTotalTimes>=0)
                .findFirst().map(d -> d.accumulated.accTotalTimes).orElse(0);
        int secondAC=secondZoneList.parallelStream().filter(d -> d.accumulated != null&&d.accumulated.accTotalTimes!=null&&d.accumulated.accTotalTimes>=0)
                .findFirst().map(d -> d.accumulated.accTotalTimes).orElse(0);
        int accTotalTimes = secondAC - firstAC;

        int firstDC=fromZoneList.parallelStream().filter(d ->d.accumulated!= null&&d.accumulated.decTotalTimes!=null&&d.accumulated.decTotalTimes>=0)
                .findFirst().map(d -> d.accumulated.decTotalTimes).orElse(0);
        int secondDC=secondZoneList.parallelStream().filter(d -> d.accumulated != null&&d.accumulated.decTotalTimes!=null&&d.accumulated.decTotalTimes>=0)
                .findFirst().map(d -> d.accumulated.decTotalTimes).orElse(0);
        int decTotalTimes = secondDC - firstDC;


        deHourTransfor.setSharpTurnCount(sharpTurnTotalTimes);
        deHourTransfor.setRapidAccelerationCount(accTotalTimes);
        deHourTransfor.setRapidDecelerationCount(decTotalTimes);

        return deHourTransfor;

    }
    public DeDayTransfor getDayDeDataAnalysisFromBigDao(String din, String dateStrFrom, String dateStrTo){
        DeDayTransfor deDayTransfor =DeDayTransfor.builder()
                .sharpTurnCount(0)
                .rapidAccelerationCount(0)
                .rapidDecelerationCount(0)
                .build();


        Date fromDate= convertDateFromStr(dateStrFrom, DateTimeUtils.TimeFormat.LONG_DATE_PATTERN_LINE);
        Date toDate= convertDateFromStr(dateStrTo, DateTimeUtils.TimeFormat.LONG_DATE_PATTERN_LINE);


        List<DataPackage> fromZoneList = cstDataFromBigDaoService.getDriveEventData(din, fromDate, toDate);
        if (CollectionUtils.isEmpty(fromZoneList)) {
            return deDayTransfor;
        }
        log.debug("find zone data from din {},time from {},time to{},data is {}",din,fromDate,toDate,fromZoneList);
        //找到下一个小时的数据
        //小时加一
        int i = 0;
        List<DataPackage> secondZoneList = null;
        while (CollectionUtils.isEmpty(secondZoneList)){
            fromDate = convertLDTToDate(convertDateoLDTT(fromDate).plusDays(1));
            toDate=convertLDTToDate(convertDateoLDTT(toDate).plusDays(1));
            secondZoneList = cstDataFromBigDaoService.getDriveEventData(din,fromDate,toDate);
            if (++i == 50) {
                break;
            }
        }
        if (CollectionUtils.isEmpty(secondZoneList)) {
            return deDayTransfor;
        }
        log.debug("find second data from din {},time from {},time to{},data is {}",din,fromDate,toDate,fromZoneList);
        int firstST=fromZoneList.parallelStream().filter(d ->d.accumulated!= null&&d.accumulated.sharpTurnTotalTimes!=null&&d.accumulated.sharpTurnTotalTimes>=0)
                .findFirst().map(d -> d.accumulated.sharpTurnTotalTimes).orElse(0);
        int secondST=secondZoneList.parallelStream().filter(d -> d.accumulated != null&&d.accumulated.sharpTurnTotalTimes!=null&&d.accumulated.sharpTurnTotalTimes>=0)
                .findFirst().map(d -> d.accumulated.sharpTurnTotalTimes).orElse(0);
        int sharpTurnTotalTimes = secondST - firstST;

        int firstAC=fromZoneList.parallelStream().filter(d ->d.accumulated!= null&&d.accumulated.accTotalTimes!=null&&d.accumulated.accTotalTimes>=0)
                .findFirst().map(d -> d.accumulated.accTotalTimes).orElse(0);
        int secondAC=secondZoneList.parallelStream().filter(d -> d.accumulated != null&&d.accumulated.accTotalTimes!=null&&d.accumulated.accTotalTimes>=0)
                .findFirst().map(d -> d.accumulated.accTotalTimes).orElse(0);
        int accTotalTimes = secondAC - firstAC;

        int firstDC=fromZoneList.parallelStream().filter(d ->d.accumulated!= null&&d.accumulated.decTotalTimes!=null&&d.accumulated.decTotalTimes>=0)
                .findFirst().map(d -> d.accumulated.decTotalTimes).orElse(0);
        int secondDC=secondZoneList.parallelStream().filter(d -> d.accumulated != null&&d.accumulated.decTotalTimes!=null&&d.accumulated.decTotalTimes>=0)
                .findFirst().map(d -> d.accumulated.decTotalTimes).orElse(0);
        int decTotalTimes = secondDC - firstDC;


        deDayTransfor.setSharpTurnCount(sharpTurnTotalTimes);
        deDayTransfor.setRapidAccelerationCount(accTotalTimes);
        deDayTransfor.setRapidDecelerationCount(decTotalTimes);

        return deDayTransfor;

    }



    public  double round(BigDecimal bigDecimal, int scale) {
        if (scale < 0) {
            return bigDecimal.doubleValue();
        }
        return bigDecimal.setScale(scale, RoundingMode.HALF_UP).doubleValue();
    }


    public IntegrateHourVo createIntegrate(String carId, String dateFromStr, String dateToStr){
        if (StringUtils.isBlank(carId)) {
            return null;
        }

        log.debug("big data and dubbo carid {},datefrom {},dateTo {}", carId, dateFromStr, dateToStr);


        CarInfo carInfo=carInfoService.getCarInfoFromRPC(carId);
        if (carInfo == null) {
            log.info("can not find car info with {}",carId);
            return null;
        }
        log.debug("get data from carid:{}",carInfo);

        IntegrateHourVo integrateHourVo = IntegrateHourVo.builder()
                .obdHourTransfor(getHourObdDataAnalysisFromBigDao(carInfo.getDin(),dateFromStr,dateToStr))
                .deHourTransfor(getHourDeDataAnalysisFromBigDao(carInfo.getDin(),dateFromStr,dateToStr))
                .gpsHourTransfor(getHourGpsDataAnalysisFromBigDao(carInfo.getDin(),dateFromStr,dateToStr))
                .traceHourTransfor(getHourTraceDataAnalysisFromBigDao(carInfo.getDin(),dateFromStr,dateToStr))
                .traceDeleteHourTransfor(getHourTraceDeleteDataAnalysisFromBigDao(carInfo.getDin(),dateFromStr,dateToStr))
                .voltageHourTransfor(getHourVoltageDataAnalysisFromBigDao(carInfo.getDin(),dateFromStr,dateToStr))
                .amHourTransfor(getHourAmDataAnalysisFromBigDao(carInfo.getDin(),dateFromStr,dateToStr))
                .build();

        return integrateHourVo;
    }


    public IntegrateDayVo createDayIntegrated(String carId, String dateFromStr, String dateToStr){
        if (StringUtils.isBlank(carId)) {
            return null;
        }



        CarInfo carInfo=carInfoService.getCarInfoFromRPC(carId);
        if (carInfo == null) {
            log.info("can not find car info with {}",carId);
            return null;
        }
        log.debug("get data from carid:{}",carInfo);

        IntegrateDayVo integrateDayVo= IntegrateDayVo.builder()
                .obdDayTransfor(getDayObdDataAnalysisFromBigDao(carInfo.getDin(),dateFromStr,dateToStr))
                .deDayTransfor(getDayDeDataAnalysisFromBigDao(carInfo.getDin(),dateFromStr,dateToStr))
                .gpsDayTransfor(getDayGpsDataAnalysisFromBigDao(carInfo.getDin(),dateFromStr,dateToStr))
                .traceDayTransfor(getDayTraceDataAnalysisFromBigDao(carInfo.getDin(),dateFromStr,dateToStr))
                .traceDeleteDayTransfor(getDayTraceDeleteDataAnalysisFromBigDao(carInfo.getDin(),dateFromStr,dateToStr))
                .voltageDayTransfor(getDayVoltageDataAnalysisFromBigDao(carInfo.getDin(),dateFromStr,dateToStr))
                .amDayTransfor(getDayAmDataAnalysisFromBigDao(carInfo.getDin(),dateFromStr,dateToStr))
                .build();

        return integrateDayVo;
    }

}

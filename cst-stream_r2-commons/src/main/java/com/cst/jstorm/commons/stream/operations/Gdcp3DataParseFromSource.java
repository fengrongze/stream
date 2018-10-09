package com.cst.jstorm.commons.stream.operations;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.tuple.Values;
import com.cst.gdcp.common.sdc.CSTRTDataStaticCatalog;
import com.cst.gdcp.common.svcfw.msgpck.CSTMsgPackage;
import com.cst.gdcp.common.svcfw.msgpck.CSTMsgPacker;
import com.cst.gdcp.factory.kafka.dto.car.BaseCarDto;
import com.cst.gdcp.factory.kafka.dto.din.*;
import com.cst.gdcp.protobuf.common.CSTAlarmTypeConst;
import com.cst.gdcp.protobuf.common.CSTTravel;
import com.cst.gdcp.protobuf.common.CSTVoltageData;
import com.cst.gdcp.rthandler.datamgr.CSTRTDataHandler;
import com.cst.gdcp.rthandler.datamgr.data.*;
import com.cst.jstorm.commons.stream.constants.EventType;
import com.cst.jstorm.commons.stream.constants.LogMsgDefine;
import com.cst.jstorm.commons.stream.constants.StreamKey;
import com.cst.stream.common.CstConstants;
import com.cst.stream.common.DateTimeUtils;
import com.cst.stream.common.JsonHelper;
import com.cst.stream.stathour.am.AmHourSource;
import com.cst.stream.stathour.de.DeHourSource;
import com.cst.stream.stathour.dormancy.DormancySource;
import com.cst.stream.stathour.gps.GpsHourSource;
import com.cst.stream.stathour.obd.ObdHourSource;
import com.cst.stream.stathour.trace.TraceHourSource;
import com.cst.stream.stathour.tracedelete.TraceDeleteHourSource;
import com.cst.stream.stathour.voltage.VoltageHourSource;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang.time.DateFormatUtils;

import java.time.LocalDateTime;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static com.cst.jstorm.commons.stream.operations.InvalidDataCharge.invalidUpTime;
import static com.cst.stream.common.MathUtils.divideTransfor;


/**
 * @author Johnney.Chiu
 * create on 2018/5/22 18:34
 * @Description
 * @title
 */

@Slf4j
public class Gdcp3DataParseFromSource {

    private static final long MILL_UNIT = 1000L;
    private static final long THOUSAND = 1000L;


    public static  void goDe(String data,SpoutOutputCollector collector) {
        if(StringUtils.isBlank(data))
            return;
        JsonObject jsonObject = (JsonObject) new JsonParser().parse(data);
        try {
            String carId = jsonObject.get("carId").getAsString();
            long dtm = jsonObject.get("dtm").getAsLong();
            if(jsonObject==null||StringUtils.isBlank(carId)||jsonObject.get("data")==null||dtm<0)
                return;
            if(invalidUpTime(carId,"eventType",dtm*MILL_UNIT))
                return;

            int eventType = jsonObject.get("data").getAsJsonObject().get("etyp").getAsInt();


            AmHourSource amHourSource = null;
            //三急事件
            DeHourSource deHourSource = null;
            switch (eventType){
                case EventType.ALARM_CAR_IGNITION:
                {
                    amHourSource = AmHourSource.builder()
                            .carId(carId)
                            .time(dtm*MILL_UNIT)
                            .alarmType(CSTAlarmTypeConst.ALARM_CAR_IGNITION)
                            .build();
                    JsonObject gpsInfo = jsonObject.get("data").getAsJsonObject().get("gps").getAsJsonObject();
                    if (gpsInfo != null) {
                        amHourSource.setLongitude(gpsInfo.get("lng").getAsDouble());
                        amHourSource.setLatitude(gpsInfo.get("lat").getAsDouble());
                    }
                    break;

                }
                case EventType.ALARM_CAR_FLAMEOUT:
                {
                    amHourSource = AmHourSource.builder()
                            .carId(carId)
                            .time(dtm*MILL_UNIT)
                            .alarmType(CSTAlarmTypeConst.ALARM_CAR_FLAMEOUT)
                            .build();
                    break;
                }
                case EventType.ALARM_CAR_INSERT:
                {
                    long pltm;
                    if (jsonObject.get("data").getAsJsonObject().get("ext").getAsJsonObject() == null) {
                        pltm = 0L;
                    }else{
                        pltm=jsonObject.get("data").getAsJsonObject().get("ext").getAsJsonObject().get("pltm").getAsLong() * MILL_UNIT;
                    }
                    amHourSource = AmHourSource.builder()
                            .carId(carId)
                            .time(dtm*MILL_UNIT)
                            .alarmType(CSTAlarmTypeConst.ALARM_TERMINAL_INSERT)
                            .pullout(pltm)
                            .build();

                    break;
                }
                case EventType.ALARM_FATIGUE_DRIVING:
                {
                    amHourSource = AmHourSource.builder()
                            .carId(carId)
                            .time(dtm*MILL_UNIT)
                            .alarmType(CSTAlarmTypeConst.ALARM_FATIGUE_DRIVING_PARA)
                            .build();
                    break;
                }
                case EventType.ALARM_COLLISION:
                {
                    amHourSource = AmHourSource.builder()
                            .carId(carId)
                            .time(dtm*MILL_UNIT)
                            .alarmType(CSTAlarmTypeConst.ALARM_COLLISION)
                            .build();
                    break;
                }
                case EventType.ALARM_OVERSPEED:
                {
                    amHourSource = AmHourSource.builder()
                            .carId(carId)
                            .time(dtm*MILL_UNIT)
                            .alarmType(CSTAlarmTypeConst.ALARM_OVERSPEED)
                            .build();
                    break;
                }
                case EventType.ALARM_DISAPPEAR_STATE:
                {
                    amHourSource = AmHourSource.builder()
                            .carId(carId)
                            .time(dtm*MILL_UNIT)
                            .alarmType(CSTAlarmTypeConst.ALARM_FATIGUE_DRIVING_PARA)
                            .build();
                    break;
                }
                case EventType.ALARM_TERMINAL_PULL:
                {
                    amHourSource = AmHourSource.builder()
                            .carId(carId)
                            .time(dtm*MILL_UNIT)
                            .alarmType(CSTAlarmTypeConst.ALARM_TERMINAL_PULL)
                            .build();
                    break;
                }
                case EventType.EVENT_RAPID_ACCELERATION:
                {
                    if(jsonObject.get("data").getAsJsonObject().get("ext")==null)
                        return;
                    deHourSource=DeHourSource.builder()
                            .carId(carId)
                            .time(dtm*MILL_UNIT)
                            .rapidAccelerationCounts(jsonObject.get("data").getAsJsonObject().get("ext").getAsJsonObject().get("acl").getAsInt())
                            .rapidDecelerationCounts(jsonObject.get("data").getAsJsonObject().get("ext").getAsJsonObject().get("dec").getAsInt())
                            .sharpTurnCounts(jsonObject.get("data").getAsJsonObject().get("ext").getAsJsonObject().get("swv").getAsInt())
                            .build();
                    break;
                }
                case EventType.EVENT_RAPID_DECELERATION:
                {
                    if(jsonObject.get("data").getAsJsonObject().get("ext")==null)
                        return;
                    deHourSource=DeHourSource.builder()
                            .carId(carId)
                            .time(dtm*MILL_UNIT)
                            .rapidAccelerationCounts(jsonObject.get("data").getAsJsonObject().get("ext").getAsJsonObject().get("acl").getAsInt())
                            .rapidDecelerationCounts(jsonObject.get("data").getAsJsonObject().get("ext").getAsJsonObject().get("dec").getAsInt())
                            .sharpTurnCounts(jsonObject.get("data").getAsJsonObject().get("ext").getAsJsonObject().get("swv").getAsInt())
                            .build();
                    break;

                }
                case EventType.EVENT_SHARP_TURN:
                {
                    if(jsonObject.get("data").getAsJsonObject().get("ext")==null)
                        return;
                    deHourSource=DeHourSource.builder()
                            .carId(carId)
                            .time(dtm*MILL_UNIT)
                            .rapidAccelerationCounts(jsonObject.get("data").getAsJsonObject().get("ext").getAsJsonObject().get("acl").getAsInt())
                            .rapidDecelerationCounts(jsonObject.get("data").getAsJsonObject().get("ext").getAsJsonObject().get("dec").getAsInt())
                            .sharpTurnCounts(jsonObject.get("data").getAsJsonObject().get("ext").getAsJsonObject().get("swv").getAsInt())
                            .build();

                }
            }
            if (amHourSource != null) {
                collector.emit(StreamKey.AmStream.AM_GDCP3_HOUR_BOLT_F, new Values(JsonHelper.toStringWithoutException(amHourSource)
                        , carId));
            }
            if (deHourSource != null) {
                //三急事件
                collector.emit(StreamKey.DeStream.DE_GDCP3_HOUR_BOLT_F, new Values(JsonHelper.toStringWithoutException(deHourSource)
                        , carId));
            }

        } catch (Throwable e) {
            log.error("de parse error:{}",data,e);
        }

    }

    public static void goGPS(String data,SpoutOutputCollector collector) {
        if(StringUtils.isBlank(data))
            return;
        try {

            BaseCarDto<GpsDto> baseCarDto = JsonHelper.toBean(data, new TypeReference<BaseCarDto<GpsDto>>() {
            });
            if(baseCarDto==null||StringUtils.isBlank(baseCarDto.getCarId())||baseCarDto.getData()==null||baseCarDto.getDtm()<=0)
                return;
            GpsDto gpsDto = baseCarDto.getData();
            if(invalidUpTime(baseCarDto.getCarId(),"GPS",gpsDto.getGtm()*MILL_UNIT))
                return;
            GpsHourSource gpsData = GpsHourSource.builder()
                    .carId(baseCarDto.getCarId())
                    .time(gpsDto.getGtm()*MILL_UNIT)
                    .latitude(gpsDto.getLat())
                    .longitude(gpsDto.getLng())
                    .satellites(gpsDto.getSatl())
                    .build();
            log.debug(JsonHelper.toStringWithoutException(gpsData));
            collector.emit(StreamKey.GpsStream.GPS_GDCP3_HOUR_BOLT_F, new Values(JsonHelper.toStringWithoutException(gpsData), gpsData.getCarId()));
        } catch (Throwable e) {
            log.error("gps parse error:{}",data,e);
        }

    }

    public static void goOBD(String data,SpoutOutputCollector collector) {
        if(StringUtils.isBlank(data))
            return;
        try {
            BaseCarDto<ObdDto> baseCarDto = JsonHelper.toBean(data, new TypeReference<BaseCarDto<ObdDto>>() {
            });
            if(baseCarDto==null||StringUtils.isBlank(baseCarDto.getCarId())||baseCarDto.getData()==null||baseCarDto.getDtm()<=0)
                return;
            ObdDto obdDto = baseCarDto.getData();
            if(invalidUpTime(baseCarDto.getCarId(),"GDCP3_OBD", obdDto.getDtm()*MILL_UNIT))
                return;
            ObdHourSource obdHourSource = ObdHourSource.builder()
                    .totalDistance((float)divideTransfor(obdDto.getMil(),THOUSAND, 3 ))
                    .time(obdDto.getDtm()*MILL_UNIT)
                    .carId(baseCarDto.getCarId())
                    .din(baseCarDto.getDin())
                    .engineSpeed(obdDto.getRspd())
                    .motormeterDistance((float)obdDto.getMmil())
                    .runTotalTime((int)obdDto.getCrt().longValue())
                    .speed(obdDto.getCspd())
                    .totalFuel((float)divideTransfor(obdDto.getTful().floatValue(),THOUSAND, 3 ))
                    .build();
            collector.emit(StreamKey.ObdStream.OBD_GDCP3_HOUR_BOLT_F,
                    new Values(JsonHelper.toStringWithoutException(obdHourSource), obdHourSource.getCarId()));
            log.info("Emit gdcp3 obd: carId={}, time={}", obdHourSource.getCarId(), DateFormatUtils.format(obdHourSource.getTime(), "MM-dd HH:mm:ss.SSS"));
        } catch (Throwable e) {
            log.error("gdcp3 obd parse error:{}",data,e);
        }
    }

    public static void goElectricOBD(String data,SpoutOutputCollector collector) {
        if(StringUtils.isBlank(data))
            return;
        try {
            BaseCarDto<EObdDto.Of> baseCarOfDto = JsonHelper.toBean(data, new TypeReference<BaseCarDto<EObdDto.Of>>() {
            });
            BaseCarDto<EObdDto.Mix> baseCarMixDto = JsonHelper.toBean(data, new TypeReference<BaseCarDto<EObdDto.Mix>>() {
            });

            if(baseCarOfDto==null||StringUtils.isBlank(baseCarOfDto.getCarId())||baseCarOfDto.getData()==null||baseCarOfDto.getDtm()<=0)
                return;
            if(invalidUpTime(baseCarOfDto.getCarId(),"GDCP3_ELECTRIC_OBD", baseCarOfDto.getDtm()*MILL_UNIT))
                return;

            ObdHourSource electricObdHourSource = ObdHourSource.builder()
                    .totalFuel((float)divideTransfor(baseCarMixDto==null?0d:baseCarMixDto.getData()==null?0d:baseCarMixDto.getData().getTful(),THOUSAND, 3 ))
                    .speed((int)baseCarOfDto.getData().getCspd().floatValue())
                    .runTotalTime((int)baseCarOfDto.getData().getCrt().longValue())
                    .motormeterDistance((float)baseCarOfDto.getData().getMmil().longValue())
                    .engineSpeed(baseCarOfDto.getData().getMspd())
                    .din(baseCarOfDto.getDin())
                    .time(baseCarOfDto.getDtm()*MILL_UNIT)
                    .totalDistance((float)divideTransfor(baseCarOfDto.getData().getTmil(),THOUSAND, 3 ))
                    .build();
            collector.emit(StreamKey.ElectricObdStream.ELECTRIC_GDCP3_OBD_HOUR_BOLT_F,
                    new Values(JsonHelper.toStringWithoutException(electricObdHourSource), electricObdHourSource.getCarId()));
            log.info("Emit electric obd: carId={}, time={}", electricObdHourSource.getCarId(), DateFormatUtils.format(electricObdHourSource.getTime(), "MM-dd HH:mm:ss.SSS"));
        } catch (Throwable e) {
            log.error("electric obd parse error:{}",data,e);
        }
    }

    public static void goDeleteTrace(String msg,SpoutOutputCollector collector) {
        log.debug("trace delete value:{}",msg);
        Date now = new Date();
        try{
            if(StringUtils.isBlank(msg))
                return;
            String[] msgs = msg.split(",");
            if (msgs == null || msgs.length < 2 || StringUtils.isBlank(msgs[0]) || StringUtils.isBlank(msgs[1])||
                    invalidUpTime(msgs[0],"trace", Long.parseLong(msgs[1]))) {
                log.warn("Invalid data: {}, type=TraceDelete", msg);
                return;
            }
            TraceDeleteHourSource traceDeleteHourSource = new TraceDeleteHourSource(msgs[0],
                    Long.parseLong(msgs[1]), msgs[2],msgs[3]);
            collector.emit(StreamKey.TraceDeleteStream.TRACE_DELETE_GDCP3_HOUR_BOLT_F, new Values(JsonHelper.toStringWithoutException(traceDeleteHourSource), traceDeleteHourSource.getCarId()));
        } catch (Throwable e) {
            log.error("delete trace parse error:{}",msg,e);
        }
    }

    public static void goTrace(String data,SpoutOutputCollector collector) {
        if(StringUtils.isBlank(data))
            return;
        try{
            BaseCarDto<TravelStatisDto> travelStatisDtoBaseCarDto=JsonHelper.toBeanWithoutException(data, new TypeReference<BaseCarDto<TravelStatisDto>>(){
            });

            if(travelStatisDtoBaseCarDto==null||StringUtils.isBlank(travelStatisDtoBaseCarDto.getCarId())||travelStatisDtoBaseCarDto.getData()==null||travelStatisDtoBaseCarDto.getDtm()<=0)
                return;
            if(invalidUpTime(travelStatisDtoBaseCarDto.getCarId(),"GDCP3_TRAVEL", travelStatisDtoBaseCarDto.getDtm()*MILL_UNIT))
                return;
            if(travelStatisDtoBaseCarDto.getData().getTravelType().intValue()!=1)
                return;
            TraceHourSource traceHourSource = TraceHourSource.builder()
                    .carId(travelStatisDtoBaseCarDto.getCarId())
                    .time(travelStatisDtoBaseCarDto.getDtm()*MILL_UNIT)
                    .tripDriveTime((int)(travelStatisDtoBaseCarDto.getData().getStopTime()-travelStatisDtoBaseCarDto.getData().getStartTime()))
                    .tripDistance(travelStatisDtoBaseCarDto.getData().getTripDistance())
                    .travelUuid(travelStatisDtoBaseCarDto.getData().getTravelId())
                    .startTime(travelStatisDtoBaseCarDto.getData().getStartTime())
                    .stopTime(travelStatisDtoBaseCarDto.getData().getStopTime())
                    .travelType(travelStatisDtoBaseCarDto.getData().getTravelType())
                    .build();
            collector.emit(StreamKey.TraceStream.TRACE_GDCP3_HOUR_BOLT_F, new Values(JsonHelper.toStringWithoutException(traceHourSource),traceHourSource.getCarId()));
        } catch (Throwable e){
            log.error("trace parse error:{}",data,e);
        }
    }

    public static void goOther(String data,SpoutOutputCollector collector,Map map) {
        if(StringUtils.isBlank(data))
            return;
        try{
            JsonObject jsonObject = (JsonObject) new JsonParser().parse(data);
            if (jsonObject == null || jsonObject.get("fid").getAsInt() == 10) {
                goVoltage(data, collector);
            }
            if (jsonObject == null || jsonObject.get("fid").getAsInt() == 255) {
                goDormancy(data, collector, map);
            }
           }
        catch (Throwable e){
            log.error("voltage parse error:{}",data,e);
        }


    }

    public static void goVoltage(String data,SpoutOutputCollector collector) {
        BaseCarDto<OtherDto.Volt> voltBaseCarDto=JsonHelper.toBeanWithoutException(data, new TypeReference<BaseCarDto<OtherDto.Volt>>(){
        });
        if(voltBaseCarDto==null||StringUtils.isBlank(voltBaseCarDto.getCarId())||voltBaseCarDto.getData()==null||voltBaseCarDto.getDtm()<=0)
            return;
        if(invalidUpTime(voltBaseCarDto.getCarId(),"GDCP3_VOLTAGE", voltBaseCarDto.getDtm()*MILL_UNIT))
            return;

        VoltageHourSource voltageHourSource = VoltageHourSource.
                builder().voltage(voltBaseCarDto.getData().getVolt()).carId(voltBaseCarDto.getCarId())
                .time(voltBaseCarDto.getDtm()*MILL_UNIT).build();
        collector.emit(StreamKey.VoltageStream.VOLTAGE_GDCP3_HOUR_BOLT_F, new Values(JsonHelper.toStringWithoutException(voltageHourSource),voltageHourSource.getCarId()));

    }

    public static void goDormancy(String data,SpoutOutputCollector collector,Map map) {
        BaseCarDto<OtherDto.Dormancy> dormancyBaseCarDto=JsonHelper.toBeanWithoutException(data, new TypeReference<BaseCarDto<OtherDto.Dormancy>>(){
        });
        if(dormancyBaseCarDto==null||StringUtils.isBlank(dormancyBaseCarDto.getCarId())||dormancyBaseCarDto.getData()==null||dormancyBaseCarDto.getDtm()<=0)
            return;
        if(invalidUpTime(dormancyBaseCarDto.getCarId(),"GDCP3_DORMANCY", dormancyBaseCarDto.getDtm()*MILL_UNIT))
            return;
        String dormancyStart=(String)map.get("dormancy_start");
        String dormancyEnd=(String)map.get("dormancy_end");
        Date date = new Date(dormancyBaseCarDto.getDtm()*MILL_UNIT);
        LocalDateTime recieveLocalDateTime = DateTimeUtils.getDateTimeOfTimestamp(dormancyBaseCarDto.getDtm()*MILL_UNIT);
        LocalDateTime localDateTimeStart=DateTimeUtils.getSpecialOfDateBymillSeconds(date, dormancyStart, DateTimeUtils.TimeFormat.SHORT_TIME_PATTERN_NONE_WITHOUT_MIS);
        LocalDateTime localDateTimeEnd=DateTimeUtils.getSpecialOfDateBymillSeconds(date, dormancyEnd, DateTimeUtils.TimeFormat.SHORT_TIME_PATTERN_NONE_WITHOUT_MIS);
        if (recieveLocalDateTime.isBefore(localDateTimeStart) || recieveLocalDateTime.isAfter(localDateTimeEnd)) {
            return;
        }
        DormancySource dormancySource = DormancySource.builder()
                .carId(dormancyBaseCarDto.getCarId())
                .time(dormancyBaseCarDto.getDtm()*MILL_UNIT)
                .build();

        collector.emit(StreamKey.DormancyStream.DORMANCY_GDCP3_DAY_BOLT_F, new Values(JsonHelper.toStringWithoutException(dormancySource), dormancySource.getCarId()));
    }



}

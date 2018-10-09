package com.cst.jstorm.commons.stream.operations;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.tuple.Values;
import com.cst.gdcp.common.sdc.CSTRTDataStaticCatalog;
import com.cst.gdcp.common.svcfw.msgpck.CSTMsgPackage;
import com.cst.gdcp.common.svcfw.msgpck.CSTMsgPacker;
import com.cst.gdcp.di.bean.MileageBean;
import com.cst.gdcp.proto.cvlr.CSTDevStateSubscribeRes;
import com.cst.gdcp.protobuf.common.CSTCommonMsgTypeConst;
import com.cst.gdcp.protobuf.common.CSTTravel;
import com.cst.gdcp.protobuf.common.CSTVoltageData;
import com.cst.gdcp.rthandler.datamgr.CSTRTData;
import com.cst.gdcp.rthandler.datamgr.CSTRTDataHandler;
import com.cst.gdcp.rthandler.datamgr.data.*;
import com.cst.jstorm.commons.stream.constants.LogMsgDefine;
import com.cst.jstorm.commons.stream.constants.StreamKey;
import com.cst.stream.common.CstConstants;
import com.cst.stream.common.DateTimeUtil;
import com.cst.stream.common.DateTimeUtils;
import com.cst.stream.common.JsonHelper;
import com.cst.stream.stathour.am.AmHourSource;
import com.cst.stream.stathour.de.DeHourSource;
import com.cst.stream.stathour.dormancy.DormancySource;
import com.cst.stream.stathour.gps.GpsHourSource;
import com.cst.stream.stathour.mileage.MileageHourSource;
import com.cst.stream.stathour.obd.ObdHourSource;
import com.cst.stream.stathour.trace.TraceHourSource;
import com.cst.stream.stathour.tracedelete.TraceDeleteHourSource;
import com.cst.stream.stathour.voltage.VoltageHourSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang.time.DateFormatUtils;

import java.time.LocalDateTime;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static com.cst.jstorm.commons.stream.operations.InvalidDataCharge.invalidUpTime;


/**
 * @author Johnney.Chiu
 * create on 2018/5/22 18:34
 * @Description
 * @title
 */

@Slf4j
public class DataParseFromSource {

    private static final int INVALID_DATA = 0xFFFFFFFF;


    public static void goAm(byte[] byteArray, SpoutOutputCollector collector) {
        CSTMsgPacker packer = new CSTMsgPacker();
        List<CSTMsgPackage> packages = packer.unpack(byteArray);
        if (packages == null)
            return;
        for (CSTMsgPackage pack : packages) {
            log.debug(LogMsgDefine.AM_BEGIN_PARSE_MSG, ":" + pack.getMsgData());
            try {
                CSTRTDataHandler handler = CSTRTDataStaticCatalog.getInstance().queryDataHandler(pack.getMsgType());
                CSTRTAlarmData alarm = (CSTRTAlarmData) handler.createRTData(pack.getMsgData());
                if (alarm == null) {
                    continue;
                }
                if (alarm.getObdDevInfo() == null || StringUtils.isBlank(alarm.getObdDevInfo().getCarid())) {
                    log.warn("Invalid data base info: {}, type=AM", alarm);
                    continue;
                }

                if (invalidUpTime(alarm.getObdDevInfo().getCarid(), "AM", alarm.getObdTime()))
                    continue;
                AmHourSource amDataSource = new AmHourSource(alarm.getObdDevInfo().getCarid(),
                        alarm.getObdTime(), alarm.getAlarmType(), alarm.getTroubleCode(),
                        alarm.getGatherType(), alarm.getLongitude(), alarm.getLatitude(),
                        alarm.getObdDevInfo().getVersionType(), alarm.getPullout());
                //alarm.getPullout()获取拔出时间，插入告警有此属性
                collector.emit(StreamKey.AmStream.AM_HOUR_BOLT_F, new Values(JsonHelper.toStringWithoutException(amDataSource), amDataSource.getCarId()));
            } catch (Throwable e) {
                log.error("am parse error:{}", pack.getMsgData(), e);
            }
        }

    }

    public static void goDe(byte[] byteArray, SpoutOutputCollector collector) {
        CSTMsgPacker packer = new CSTMsgPacker();
        List<CSTMsgPackage> packages = packer.unpack(byteArray);
        if (packages == null)
            return;
        for (CSTMsgPackage pack : packages) {
            log.debug(LogMsgDefine.DE_BEGIN_PARSE_MSG, ":" + pack.getMsgData());
            try {
                CSTRTDataHandler handler = CSTRTDataStaticCatalog.getInstance().queryDataHandler(pack.getMsgType());
                CSTRTDriveEvent de = (CSTRTDriveEvent) handler.createRTData(pack.getMsgData());
                if (de == null || de.getObdDevInfo() == null)
                    continue;
                if (StringUtils.isBlank(de.getObdDevInfo().getCarid()))
                    continue;
                if (invalidUpTime(de.getObdDevInfo().getCarid(), "DE", de.getObdTime()))
                    continue;
                DeHourSource deData = new DeHourSource(de.getObdDevInfo().getCarid(), de.getObdTime(),
                        de.getGPSSpeed(), de.getActType(), de.getGatherType(), de.getAccTotalTimes(), de.getDecTotalTimes(), de.getSharpTotalTimes());
                collector.emit(StreamKey.DeStream.DE_HOUR_BOLT_F, new Values(JsonHelper.toStringWithoutException(deData), deData.getCarId()));
            } catch (Throwable e) {
                log.error("de parse error:{}", pack.getMsgData(), e);
            }
        }

    }

    public static void goGPS(byte[] byteArray, SpoutOutputCollector collector) {
        CSTMsgPacker packer = new CSTMsgPacker();
        List<CSTMsgPackage> packages = packer.unpack(byteArray);
        if (packages == null)
            return;
        for (CSTMsgPackage pack : packages) {
            log.debug(LogMsgDefine.GPS_BEGIN_PARSE_MSG, ":" + pack.getMsgData());
            try {
                CSTRTDataHandler handler = CSTRTDataStaticCatalog.getInstance().queryDataHandler(pack.getMsgType());
                CSTRTGpsData gps = (CSTRTGpsData) handler.createRTData(pack.getMsgData());
                if (gps == null || gps.getObdDevInfo() == null)
                    continue;
                if (StringUtils.isBlank(gps.getObdDevInfo().getCarid()))
                    continue;
                if (invalidUpTime(gps.getObdDevInfo().getCarid(), "GPS", gps.getObdTime()))
                    continue;
                GpsHourSource gpsData = new GpsHourSource(gps.getObdDevInfo().getCarid(), gps.getObdTime(),
                        gps.getSatellites(), gps.getLongitude(), gps.getLatitude());
                log.debug(JsonHelper.toStringWithoutException(gpsData));
                collector.emit(StreamKey.GpsStream.GPS_HOUR_BOLT_F, new Values(JsonHelper.toStringWithoutException(gpsData), gpsData.getCarId()));
            } catch (Throwable e) {
                log.error("gps parse error:{}", pack.getMsgData(), e);
            }
        }

    }

    public static void goOBD(byte[] byteArray, SpoutOutputCollector collector) {
        CSTMsgPacker packer = new CSTMsgPacker();
        List<CSTMsgPackage> packages = packer.unpack(byteArray);
        if (packages == null)
            return;
        for (CSTMsgPackage pack : packages) {
            try {
                CSTRTDataHandler handler = CSTRTDataStaticCatalog.getInstance().queryDataHandler(pack.getMsgType());
                CSTRTObdData obd = (CSTRTObdData) handler.createRTData(pack.getMsgData());
                if (obd.getObdDevInfo().getCarid() == null || "".equals(obd.getObdDevInfo().getCarid())) continue;
                if (invalidUpTime(obd.getObdDevInfo().getCarid(), "OBD", obd.getObdTime()))
                    continue;
                ObdHourSource obdHourSource = ObdHourSource.builder()
                        .din(obd.getObdDevInfo().getObdDevId())
                        .powerNum(-1F)
                        .carId(obd.getObdDevInfo().getCarid())
                        .time(obd.getObdTime())
                        .speed(obd.getCarSpeed())
                        .engineSpeed(obd.getEngineSpeed())
                        .totalDistance(obd.getTotalDistance())
                        .totalFuel(obd.getTotalFuel())
                        .runTotalTime(obd.getRunTotalTime())
                        .motormeterDistance(obd.getMotormeterDistance())
                        .build();
                collector.emit(StreamKey.ObdStream.OBD_HOUR_BOLT_F,
                        new Values(JsonHelper.toStringWithoutException(obdHourSource), obdHourSource.getCarId()));
                log.info("Emit obd: carId={}, time={}", obdHourSource.getCarId(), DateFormatUtils.format(obdHourSource.getTime(), "MM-dd HH:mm:ss.SSS"));
            } catch (Throwable e) {
                log.error("obd parse error:{}", pack.getMsgData(), e);
            }
        }
    }

    public static void goElectricOBD(byte[] byteArray, SpoutOutputCollector collector) {
        CSTMsgPacker packer = new CSTMsgPacker();
        List<CSTMsgPackage> packages = packer.unpack(byteArray);
        if (packages == null)
            return;
        for (CSTMsgPackage pack : packages) {
            try {
                CSTRTDataHandler handler = CSTRTDataStaticCatalog.getInstance().queryDataHandler(pack.getMsgType());
                CSTRTElectricObdData electricObdData = (CSTRTElectricObdData) handler.createRTData(pack.getMsgData());
                if (electricObdData.getObdDevInfo().getCarid() == null || "".equals(electricObdData.getObdDevInfo().getCarid()))
                    continue;
                if (invalidUpTime(electricObdData.getObdDevInfo().getCarid(), "ELECTRIC_OBD", electricObdData.getObdTime()))
                    continue;

                ObdHourSource electricObdHourSource = ObdHourSource.builder()
                        .motormeterDistance(electricObdData.getObdTotalMil())
                        .runTotalTime(electricObdData.getRunTotalTime())
                        .totalFuel(electricObdData.getTotalFuel())
                        .totalDistance(electricObdData.getTotalDistance())
                        .engineSpeed(electricObdData.getEngineSpeed())
                        .speed(electricObdData.getCarSpeed())
                        .time(electricObdData.getObdTime())
                        .carId(electricObdData.getObdDevInfo().getCarid())
                        .din(electricObdData.getObdDevInfo().getObdDevId())
                        .powerNum(-1F)
                        .build();
                collector.emit(StreamKey.ElectricObdStream.ELECTRIC_OBD_HOUR_BOLT_F,
                        new Values(JsonHelper.toStringWithoutException(electricObdHourSource), electricObdHourSource.getCarId()));
                log.info("Emit electric obd: carId={}, time={}", electricObdHourSource.getCarId(), DateFormatUtils.format(electricObdHourSource.getTime(), "MM-dd HH:mm:ss.SSS"));
            } catch (Throwable e) {
                log.error("electric obd parse error:{}", pack.getMsgData(), e);
            }
        }
    }

    public static void goDeleteTrace(String msg, SpoutOutputCollector collector) {
        log.debug("trace delete value:{}", msg);
        Date now = new Date();
        try {
            if (StringUtils.isBlank(msg))
                return;
            String[] msgs = msg.split(",");
            if (msgs == null || msgs.length < 2 || StringUtils.isBlank(msgs[0]) || StringUtils.isBlank(msgs[1]) ||
                    invalidUpTime(msgs[0], "trace", Long.parseLong(msgs[1]))) {
                log.warn("Invalid data: {}, type=TraceDelete", msg);
                return;
            }
            TraceDeleteHourSource traceDeleteHourSource = new TraceDeleteHourSource(msgs[0],
                    Long.parseLong(msgs[1]), msgs[2], msgs[3]);
            collector.emit(StreamKey.TraceDeleteStream.TRACE_DELETE_HOUR_BOLT_F, new Values(JsonHelper.toStringWithoutException(traceDeleteHourSource), traceDeleteHourSource.getCarId()));
        } catch (Throwable e) {
            log.error("delete trace parse error:{}", msg, e);
        }
    }

    public static void goTrace(byte[] byteArray, SpoutOutputCollector collector) {
        CSTMsgPacker packer = new CSTMsgPacker();
        List<CSTMsgPackage> packages = packer.unpack(byteArray);
        if (packages == null)
            return;
        for (CSTMsgPackage pack : packages) {
            try {
                CSTTravel.Travel data = CSTTravel.Travel.parseFrom(pack.getUnpackData());
                if (data == null)
                    continue;
                if (StringUtils.isBlank(data.getObdDevInfo().getCarid()))
                    continue;
                if (invalidUpTime(data.getObdDevInfo().getCarid(), "trace", data.getStartTime()))
                    continue;
                //start time 当做是这条轨迹上传的时间
                TraceHourSource traceDataSource =
                        new TraceHourSource(data.getObdDevInfo().getCarid(), data.getStartTime(), data.getTravelUuid(),
                                data.getStartTime(), data.getStopTime(), data.getTripDistance(), data.getTravelType(), data.getTripDriveTime());
                collector.emit(StreamKey.TraceStream.TRACE_HOUR_BOLT_F, new Values(JsonHelper.toStringWithoutException(traceDataSource), traceDataSource.getCarId()));
            } catch (Throwable e) {
                log.error("trace parse error:{}", pack.getMsgData(), e);
            }
        }
    }

    public static void goVoltage(byte[] byteArray, SpoutOutputCollector collector) {
        CSTMsgPacker packer = new CSTMsgPacker();
        List<CSTMsgPackage> packages = packer.unpack(byteArray);
        if (packages == null)
            return;
        Date now = new Date();
        for (CSTMsgPackage pack : packages) {

            log.debug("get voltage value:{}", pack.getMsgData());
            try {
                CSTRTDataHandler handler = CSTRTDataStaticCatalog.getInstance().queryDataHandler(pack.getMsgType());
                //正式环境中式这样取值的
                if (1005008 != handler.createRTData(pack.getMsgData()).getRTDataType())
                    return;
                CSTRTVoltageGatherData volData = (CSTRTVoltageGatherData) handler.createRTData(pack.getMsgData());
                if (volData == null || volData.getObdDevInfo() == null)
                    continue;
                if (StringUtils.isBlank(volData.getObdDevInfo().getCarid()))
                    continue;

                List<CSTVoltageData.VoltageData> list = volData.voltageDataList();
                //这里会有4个电压值
                VoltageHourSource voltageHourSource;
                for (CSTVoltageData.VoltageData volObj : list) {
                    if (invalidUpTime(volData.getObdDevInfo().getCarid(), "Voltage", volData.getObdTime()))
                        continue;

                    voltageHourSource = VoltageHourSource.
                            builder().voltage(volObj.getVoltage()).carId(volData.getObdDevInfo().getCarid())
                            .time(volObj.getObdTime()).build();
                    collector.emit(StreamKey.VoltageStream.VOLTAGE_HOUR_BOLT_F, new Values(JsonHelper.toStringWithoutException(voltageHourSource), voltageHourSource.getCarId()));
                }
            } catch (Throwable e) {
                log.error("voltage parse error:{}", pack.getMsgData(), e);
            }
        }

    }


    public static void goDormancy(byte[] byteArray, SpoutOutputCollector collector, Map map) {
        CSTMsgPacker packer = new CSTMsgPacker();
        List<CSTMsgPackage> packages = packer.unpack(byteArray);
        if (packages == null)
            return;
        for (CSTMsgPackage pack : packages) {
            log.debug("get dormancy value:{}", pack.getMsgData());
            try {
                CSTRTDataHandler handler = CSTRTDataStaticCatalog.getInstance().queryDataHandler(pack.getMsgType());
                if (CSTCommonMsgTypeConst.TERMINAL_REPORT_SLEEP != handler.createRTData(pack.getMsgData()).getRTDataType())
                    return;
                CSTRTData data = handler.createRTData(pack.getMsgData());

                if (StringUtils.isBlank(data.getObdDevInfo().getCarid()))
                    continue;
                if (invalidUpTime(data.getObdDevInfo().getCarid(), "Dormancy", data.getObdTime()))
                    continue;
                String dormancyStart = (String) map.get("dormancy_start");
                String dormancyEnd = (String) map.get("dormancy_end");
                Date date = new Date(data.getObdTime());
                LocalDateTime recieveLocalDateTime = DateTimeUtils.getDateTimeOfTimestamp(data.getObdTime());
                LocalDateTime localDateTimeStart = DateTimeUtils.getSpecialOfDateBymillSeconds(date, dormancyStart, DateTimeUtils.TimeFormat.SHORT_TIME_PATTERN_NONE_WITHOUT_MIS);
                LocalDateTime localDateTimeEnd = DateTimeUtils.getSpecialOfDateBymillSeconds(date, dormancyEnd, DateTimeUtils.TimeFormat.SHORT_TIME_PATTERN_NONE_WITHOUT_MIS);
                if (recieveLocalDateTime.isBefore(localDateTimeStart) || recieveLocalDateTime.isAfter(localDateTimeEnd)) {
                    return;
                }
                DormancySource dormancySource = DormancySource.builder()
                        .carId(data.getObdDevInfo().getCarid())
                        .time(data.getObdTime())
                        .build();

                collector.emit(StreamKey.DormancyStream.DORMANCY_DAY_BOLT_F, new Values(JsonHelper.toStringWithoutException(dormancySource), dormancySource.getCarId()));
            } catch (Throwable e) {
                log.error("dormancy parse error:{}", pack.getMsgData(), e);
            }
        }
    }

    public static void goMileageData(byte[] byteArray, SpoutOutputCollector collector) {
        CSTMsgPacker packer = new CSTMsgPacker();
        List<CSTMsgPackage> packages = packer.unpack(byteArray);
        if (packages == null)
            return;
        for (CSTMsgPackage pack : packages) {
            log.debug(LogMsgDefine.MILEAGE_BEGIN_PARSE_MSG, ":" + pack.getMsgData());
            try {
                CSTRTDataHandler handler = CSTRTDataStaticCatalog.getInstance().queryDataHandler(pack.getMsgType());
                CSTRTMileageData mileageData = (CSTRTMileageData) handler.createRTData(pack.getMsgData());
                if (mileageData == null || mileageData.getObdDevInfo() == null)
                    continue;
                if (StringUtils.isBlank(mileageData.getObdDevInfo().getCarid()))
                    continue;
                if (invalidUpTime(mileageData.getObdDevInfo().getCarid(), "MILEAGE_DATA", mileageData.getObdTime()))
                    continue;
                if (mileageData.getObdTotalDistance() == INVALID_DATA) {
                    log.info("mileage obd distance invalid:{},{},{}",mileageData.getObdDevInfo().getCarid(),
                            mileageData.getObdTime(),mileageData.getObdTotalDistance());
                    continue;
                }
                if (mileageData.getTotalFuel() == INVALID_DATA) {
                    log.info("mileage fuel invalid:{},{},{}",mileageData.getObdDevInfo().getCarid()
                            ,mileageData.getObdTime(),mileageData.getTotalFuel());
                    continue;
                }
                MileageHourSource mileageHourSource = MileageHourSource.builder()
                        .carId(mileageData.getObdDevInfo().getCarid())
                        .obdSpeed(mileageData.getCarObdSpeed())
                        .gpsSpeed(mileageData.getCarGpsSpeed())
                        .milGpsTotalDistance((double) mileageData.getGpsTotalDistance() / 1000)
                        .milObdTotalDistance((double) mileageData.getObdTotalDistance() / 1000)
                        .milRunTotalTime(mileageData.getTotalRunTime())
                        .milTotalFuel((double) mileageData.getTotalFuel() / 1000)
                        .panelDistance((mileageData.getObdPanelDistance() < 0) ? -1D : mileageData.getObdPanelDistance())
                        .time(mileageData.getObdTime())
                        .build();
                log.debug("gdcp mileage original data:obdspeed {},gpsspeed{},obd total {},gps total {}," +
                                "runtotaltime: {},total fuel :{},panel {}", mileageData.getCarObdSpeed(), mileageData.getCarGpsSpeed()
                        , mileageData.getObdTotalDistance(), mileageData.getGpsTotalDistance(), mileageData.getTotalRunTime(),
                        mileageData.getTotalFuel(), mileageData.getObdPanelDistance());
                log.debug(JsonHelper.toStringWithoutException(mileageHourSource));
                collector.emit(StreamKey.MileageStream.MILEAGE_HOUR_BOLT_F,
                        new Values(JsonHelper.toStringWithoutException(mileageHourSource), mileageHourSource.getCarId()));
            } catch (Throwable e) {
                log.error("mileage parse error:{}", pack.getMsgData(), e);
            }
        }

    }


}

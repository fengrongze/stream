package com.cst.bigdata.service.integrate;

import com.cst.bigdata.config.props.FileProperties;
import com.cst.bigdata.domain.mybatis.validatedata.IntegrateDayVo;
import com.cst.bigdata.domain.mybatis.validatedata.IntegrateHourVo;
import com.cst.stream.common.DateTimeUtil;
import com.cst.stream.common.HBaseTable;
import com.cst.stream.common.HbaseColumn;
import com.cst.stream.common.StreamTypeDefine;
import com.cst.stream.stathour.CstStreamBaseResult;
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
import org.apache.commons.lang.math.NumberUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Johnney.Chiu
 * create on 2018/5/18 15:20
 * @Description
 * @title
 */

@Service
@Slf4j
public class StreamValidateIntegrateService {


    @Autowired
    private CstStreamHourIntegrateService cstStreamHourIntegrateService;

    @Autowired
    private CstStreamDayIntegrateService cstStreamDayIntegrateService;

    @Autowired
    private CstStramDataValidateIntegratedService cstStramDataValidateIntegratedService;

    public static final String DATE_FROM= "%s %s%s:00:00";
    public static final String DATE_TO= "%s %s%s:59:59";

    public static final String DAY_DATE_FROM= "%s 00:00:00";
    public static final String DAY_DATE_TO= "%s 23:59:59";
    @Autowired
    private FileProperties fileProperties;

    public File writeAndExportHourData(List<IntegrateHourVo> list,String myFileName) {

        File file = new File(fileProperties.getOutPath(),myFileName);
        try {
            writeData(list, file);
        } catch (IOException e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
        return file;
    }

    public File writeAndExportHourDataWithPer(List<IntegrateHourVo> list,String myFileName) {

        File file = new File(fileProperties.getOutPath(),myFileName);
        try {
            writeDataPer(list, file);
        } catch (IOException e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
        return file;
    }
    public File writeAndExportHourDataWithOriginalDataPer(List<IntegrateHourVo> list,String myFileName) {

        File file = new File(fileProperties.getOutPath(),myFileName);
        try {
            writeDataPerOriginal(list, file);
        } catch (IOException e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
        return file;
    }

    public File writeAndExportDayData(List<IntegrateDayVo> list,String myFileName) {

        File file = new File(fileProperties.getOutPath(),myFileName);
        try {
            writeDayData(list, file);
        } catch (IOException e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
        return file;
    }
    public File writeAndExportDayDataWithPer(List<IntegrateDayVo> list,String myFileName) {

        File file = new File(fileProperties.getOutPath(),myFileName);
        try {
            writeDayDataWithPer(list, file);
        } catch (IOException e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
        return file;
    }

    public File writeAndExportDayDataWithPerAndOriginal(List<IntegrateDayVo> list,String myFileName) {

        File file = new File(fileProperties.getOutPath(),myFileName);
        try {
            writeDayDataWithPerAndOriginal(list, file);
        } catch (IOException e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
        return file;
    }

    public void writeData(List<IntegrateHourVo> list,File file)throws IOException {
        RandomAccessFile randomFile = null;
        try {
            randomFile = new RandomAccessFile(file, "rw");
            randomFile.seek(randomFile.length());
            StringBuffer sb = new StringBuffer();
            sb.append("flag,车的ID,小时,行驶里程,耗油量,行驶时间,最大速度,点火次数,熄火次数,插入次数,急加速次数,急减速次数,急转弯次数, 连接次数,是否更换车机,最大搜星数,最低电瓶电压,最高电瓶电压,是否开车,是否疲劳驾驶,是否上了高速,拔出时长,车是否在外地,是否夜间开车,单次最大行驶时间,是否失联,碰撞告警次数,超速告警次数,与平台交互次数,该小时gps上报次数").append("\n");
            randomFile.write(sb.toString().getBytes());

            for (IntegrateHourVo integrateHourVo : list) {
                sb.setLength(0);
                //old
                if(integrateHourVo.getOriginalData()!=null&&integrateHourVo.getOriginalData().length>0) {
                    sb.append("old").append(",").append(Arrays.stream(integrateHourVo.getOriginalData()).collect(Collectors.joining(","))).append("\n");
                    randomFile.write(sb.toString().getBytes());
                    sb.setLength(0);
                }
                //new
                sb.append("new").append(",");
                sb.append(integrateHourVo.getObdHourTransfor().getCarId()).append(",");
                sb.append(integrateHourVo.getOriginalData()[1]).append(",");
                sb.append(integrateHourVo.getObdHourTransfor().getMileage()).append(",");
                sb.append(integrateHourVo.getObdHourTransfor().getFuel()).append(",");
                sb.append(integrateHourVo.getObdHourTransfor().getDuration()).append(",");
                sb.append(integrateHourVo.getObdHourTransfor().getMaxSpeed()).append(",");
                sb.append(integrateHourVo.getAmHourTransfor().getIgnition()).append(",");
                sb.append(integrateHourVo.getAmHourTransfor().getFlameOut()).append(",");
                sb.append(integrateHourVo.getAmHourTransfor().getInsertNum()).append(",");
                sb.append(integrateHourVo.getDeHourTransfor().getRapidAccelerationCount()).append(",");
                sb.append(integrateHourVo.getDeHourTransfor().getRapidDecelerationCount()).append(",");
                sb.append(integrateHourVo.getDeHourTransfor().getSharpTurnCount()).append(",");

                sb.append("未知").append(",");

                sb.append(integrateHourVo.getObdHourTransfor().getDinChange()).append(",");
                sb.append(integrateHourVo.getGpsHourTransfor().getMaxSatelliteNum()).append(",");
                sb.append(integrateHourVo.getVoltageHourTransfor().getMinVoltage()).append(",");
                sb.append(integrateHourVo.getVoltageHourTransfor().getMaxVoltage()).append(",");
                sb.append(integrateHourVo.getObdHourTransfor().getIsDrive()).append(",");
                sb.append(integrateHourVo.getAmHourTransfor().getIsFatigue()).append(",");
                sb.append(integrateHourVo.getObdHourTransfor().getIsHighSpeed()).append(",");
                sb.append(integrateHourVo.getAmHourTransfor().getPulloutTimes()).append(",");
                sb.append(integrateHourVo.getGpsHourTransfor().getIsNonLocal()).append(",");
                sb.append("未知").append(",");
                sb.append("未知").append(",");
                sb.append(integrateHourVo.getAmHourTransfor().getIsMissing()).append(",");
                sb.append(integrateHourVo.getAmHourTransfor().getCollision()).append(",");
                sb.append(integrateHourVo.getAmHourTransfor().getOverSpeed()).append(",");
                sb.append("未知").append(",");
                sb.append(integrateHourVo.getGpsHourTransfor().getGpsCount()).append(",\n");
                randomFile.write(sb.toString().getBytes());
            }
        }catch (Exception e) {
            log.error("文件写出异常",e);
        }finally {
            if (randomFile != null) {
                try {
                    randomFile.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void writeDataPer(List<IntegrateHourVo> list,File file)throws IOException {
        RandomAccessFile randomFile = null;
        try {
            randomFile = new RandomAccessFile(file, "rw");
            randomFile.seek(randomFile.length());
            StringBuffer sb = new StringBuffer();
            sb.append("flag,车的ID,小时,行驶里程,耗油量,行驶时间,最大速度,点火次数,熄火次数,插入次数,急加速次数,急减速次数,急转弯次数, 连接次数,是否更换车机,最大搜星数,最低电瓶电压,最高电瓶电压,是否开车,是否疲劳驾驶,是否上了高速,拔出时长,车是否在外地,是否夜间开车,单次最大行驶时间,是否失联,碰撞告警次数,超速告警次数,与平台交互次数,该小时gps上报次数").append("\n");
            randomFile.write(sb.toString().getBytes());
            String[] perStr=null;
            for (IntegrateHourVo integrateHourVo : list) {
                if(perStr==null) {
                    perStr = new String[integrateHourVo.getOriginalData().length];
                }
                sb.setLength(0);
                //old
                if(integrateHourVo.getOriginalData()!=null&&integrateHourVo.getOriginalData().length>0) {
                    sb.append("old").append(",").append(Arrays.stream(integrateHourVo.getOriginalData()).collect(Collectors.joining(","))).append("\n");
                    randomFile.write(sb.toString().getBytes());
                    sb.setLength(0);
                }
                //new
                sb.append("new").append(",");
                sb.append(integrateHourVo.getObdHourTransfor().getCarId()).append(",");
                sb.append(integrateHourVo.getOriginalData()[1]).append(",");
                sb.append(integrateHourVo.getObdHourTransfor().getMileage()).append(",");
                sb.append(integrateHourVo.getObdHourTransfor().getFuel()).append(",");
                sb.append(integrateHourVo.getObdHourTransfor().getDuration()).append(",");
                sb.append(integrateHourVo.getObdHourTransfor().getMaxSpeed()).append(",");
                sb.append(integrateHourVo.getAmHourTransfor().getIgnition()).append(",");
                sb.append(integrateHourVo.getAmHourTransfor().getFlameOut()).append(",");
                sb.append(integrateHourVo.getAmHourTransfor().getInsertNum()).append(",");
                sb.append(integrateHourVo.getDeHourTransfor().getRapidAccelerationCount()).append(",");
                sb.append(integrateHourVo.getDeHourTransfor().getRapidDecelerationCount()).append(",");
                sb.append(integrateHourVo.getDeHourTransfor().getSharpTurnCount()).append(",");
                sb.append("未知").append(",");

                sb.append(integrateHourVo.getObdHourTransfor().getDinChange()).append(",");
                sb.append(integrateHourVo.getGpsHourTransfor().getMaxSatelliteNum()).append(",");
                sb.append(integrateHourVo.getVoltageHourTransfor().getMinVoltage()).append(",");
                sb.append(integrateHourVo.getVoltageHourTransfor().getMaxVoltage()).append(",");
                sb.append(integrateHourVo.getObdHourTransfor().getIsDrive()).append(",");
                sb.append(integrateHourVo.getAmHourTransfor().getIsFatigue()).append(",");
                sb.append(integrateHourVo.getObdHourTransfor().getIsHighSpeed()).append(",");
                sb.append(integrateHourVo.getAmHourTransfor().getPulloutTimes()).append(",");
                sb.append(integrateHourVo.getGpsHourTransfor().getIsNonLocal()).append(",");
                sb.append("未知").append(",");
                sb.append("未知").append(",");
                sb.append(integrateHourVo.getAmHourTransfor().getIsMissing()).append(",");
                sb.append(integrateHourVo.getAmHourTransfor().getCollision()).append(",");
                sb.append(integrateHourVo.getAmHourTransfor().getOverSpeed()).append(",");
                sb.append("未知").append(",");
                sb.append(integrateHourVo.getGpsHourTransfor().getGpsCount()).append(",\n");
                randomFile.write(sb.toString().getBytes());

                sb.setLength(0);
                //old
                if(integrateHourVo.getOriginalData()!=null&&integrateHourVo.getOriginalData().length>0) {
                    perStr[0] = integrateHourVo.getOriginalData()[0];
                    perStr[1]=integrateHourVo.getOriginalData()[1];
                    perStr[2]=percentGet(NumberUtils.toFloat(integrateHourVo.getOriginalData()[2],0),integrateHourVo.getObdHourTransfor().getMileage());
                    perStr[3]=percentGet(NumberUtils.toFloat(integrateHourVo.getOriginalData()[3]),integrateHourVo.getObdHourTransfor().getFuel());
                    perStr[4]=percentGet(NumberUtils.toDouble(integrateHourVo.getOriginalData()[4]),integrateHourVo.getObdHourTransfor().getDuration());
                    perStr[5]=percentGet(NumberUtils.toFloat(integrateHourVo.getOriginalData()[5]),integrateHourVo.getObdHourTransfor().getMaxSpeed());
                    perStr[6]=percentGet(NumberUtils.toDouble(integrateHourVo.getOriginalData()[6]),integrateHourVo.getAmHourTransfor().getIgnition());
                    perStr[7]=percentGet(NumberUtils.toDouble(integrateHourVo.getOriginalData()[7]),integrateHourVo.getAmHourTransfor().getFlameOut());
                    perStr[8]=percentGet(NumberUtils.toDouble(integrateHourVo.getOriginalData()[8]),integrateHourVo.getAmHourTransfor().getInsertNum());
                    perStr[9]=percentGet(NumberUtils.toDouble(integrateHourVo.getOriginalData()[9]),integrateHourVo.getDeHourTransfor().getRapidAccelerationCount());
                    perStr[10]=percentGet(NumberUtils.toDouble(integrateHourVo.getOriginalData()[10]),integrateHourVo.getDeHourTransfor().getRapidDecelerationCount());
                    perStr[11]=percentGet(NumberUtils.toDouble(integrateHourVo.getOriginalData()[11]),integrateHourVo.getDeHourTransfor().getSharpTurnCount());
                    perStr[12] = "未知";
                    perStr[13]=percentGet(NumberUtils.toDouble(integrateHourVo.getOriginalData()[13]),integrateHourVo.getObdHourTransfor().getDinChange());
                    perStr[14]=percentGet(NumberUtils.toDouble(integrateHourVo.getOriginalData()[14]),integrateHourVo.getGpsHourTransfor().getMaxSatelliteNum());
                    perStr[15]=percentGet(NumberUtils.toFloat(integrateHourVo.getOriginalData()[15]),integrateHourVo.getVoltageHourTransfor().getMinVoltage());
                    perStr[16]=percentGet(NumberUtils.toFloat(integrateHourVo.getOriginalData()[16]),integrateHourVo.getVoltageHourTransfor().getMaxVoltage());
                    perStr[17]=percentGet(NumberUtils.toDouble(integrateHourVo.getOriginalData()[17]),integrateHourVo.getObdHourTransfor().getIsDrive());
                    perStr[18]=percentGet(NumberUtils.toDouble(integrateHourVo.getOriginalData()[18]),integrateHourVo.getAmHourTransfor().getIsFatigue());
                    perStr[19]=percentGet(NumberUtils.toDouble(integrateHourVo.getOriginalData()[19]),integrateHourVo.getObdHourTransfor().getIsHighSpeed());
                    perStr[20]=percentGet(NumberUtils.toFloat(integrateHourVo.getOriginalData()[20]),integrateHourVo.getAmHourTransfor().getPulloutTimes());
                    perStr[21]=percentGet(NumberUtils.toDouble(integrateHourVo.getOriginalData()[21]),integrateHourVo.getGpsHourTransfor().getIsNonLocal());
                    perStr[22]="未知";
                    perStr[23] = "未知";
                    perStr[24]=percentGet(NumberUtils.toDouble(integrateHourVo.getOriginalData()[24]),integrateHourVo.getAmHourTransfor().getIsMissing());
                    perStr[25]=percentGet(NumberUtils.toDouble(integrateHourVo.getOriginalData()[25]),integrateHourVo.getAmHourTransfor().getCollision());
                    perStr[26]=percentGet(NumberUtils.toDouble(integrateHourVo.getOriginalData()[26]),integrateHourVo.getAmHourTransfor().getOverSpeed());
                    perStr[27] = "未知";
                    perStr[28]=percentGet(NumberUtils.toDouble(integrateHourVo.getOriginalData()[28]),integrateHourVo.getGpsHourTransfor().getGpsCount());
                    sb.append("percent").append(",").append(Arrays.stream(perStr).collect(Collectors.joining(","))).append("\n");
                    randomFile.write(sb.toString().getBytes());
                }

            }
        }catch (Exception e) {
            log.error("文件写出异常");
            e.printStackTrace();
        }finally {
            if (randomFile != null) {
                try {
                    randomFile.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    public void writeDataPerOriginal(List<IntegrateHourVo> list,File file)throws IOException {
        RandomAccessFile randomFile = null;
        try {
            randomFile = new RandomAccessFile(file, "rw");
            randomFile.seek(randomFile.length());
            StringBuffer sb = new StringBuffer();
            sb.append("车的ID,小时,old行驶里程,new行驶里程,old_per,orignal行驶里程,new_per,old耗油量,new耗油量,old_per,orignal耗油量,new_per,old行驶时间,new行驶时间,old_per,orignal行驶时间,new_per,old最大速度,new最大速度,old_per,orignal最大速度,new_per,old点火次数,new点火次数,old_per,orignal点火次数,new_per,old熄火次数,new熄火次数,old_per,orignal熄火次数,new_per,old插入次数,new插入次数,old_per,orignal插入次数,new_per,old急加速次数,new急加速次数,old_per,orignal急加速次数,new_per,old急减速次数,new急减速次数,old_per,orignal急减速次数,new_per,old急转弯次数,new急转弯次数,old_per,orignal急转弯次数,new_per,old连接次数,new连接次数,old_per,orignal连接次数,new_per,old是否更换车机,new是否更换车机,old_per,orignal是否更换车机,new_per,old最大搜星数,new最大搜星数,old_per,orignal最大搜星数,new_per,old最低电瓶电压,new最低电瓶电压,old_per,orignal最低电瓶电压,new_per,old最高电瓶电压,new最高电瓶电压,old_per,orignal最高电瓶电压,new_per,old是否开车,new是否开车,old_per,orignal是否开车,new_per,old是否疲劳驾驶,new是否疲劳驾驶,old_per,orignal是否疲劳驾驶,new_per,old高速,new高速,old_per,orignal高速,new_per,old拔出时长,new拔出时长,old_per,orignal拔出时长,new_per,old车是否在外地,new车是否在外地,old_per,orignal车是否在外地,new_per,old夜间,new夜间,old_per,orignal夜间,new_per,old单次最大行驶时间,new单次最大行驶时间,old_per,orignal单次最大行驶时间,new_per,old是否失联,new是否失联,old_per,orignal是否失联,new_per,old碰撞告警次数,new碰撞告警次数,old_per,orignal碰撞告警次数,new_per,old超速告警次数,new超速告警次数,old_per,orignal超速告警次数,new_per,old与平台交互次数,new与平台交互次数,old_per,orignal与平台交互次数,new_per,old小时gps上报次数,new小时gps上报次数,old_per,orignal小时gps上报次数,new_per").append("\n");
            randomFile.write(sb.toString().getBytes());
            for (IntegrateHourVo integrateHourVo : list) {
                log.info("integratedHour vo  is {}", integrateHourVo);
                if (integrateHourVo.getOriginalHourVo() == null ) {
                    log.info("OriginalHourVo is {}");
                    continue;
                }
                if( integrateHourVo.getOriginalHourVo().getObdHourTransfor() == null){
                    continue;
                }
                sb.setLength(0);
                sb.append(integrateHourVo.getOriginalData()[0]).append(",");
                sb.append(integrateHourVo.getOriginalData()[1]).append(",");
                //里程
                sb.append(integrateHourVo.getOriginalData()[2]).append(",");
                sb.append(integrateHourVo.getObdHourTransfor().getMileage()).append(",");
                sb.append(percentGet(NumberUtils.toFloat(integrateHourVo.getOriginalData()[2], 0), integrateHourVo.getObdHourTransfor().getMileage())).append(",");
                sb.append(integrateHourVo.getOriginalHourVo().getObdHourTransfor().getMileage()).append(",");
                sb.append(percentGet(integrateHourVo.getOriginalHourVo().getObdHourTransfor().getMileage(), integrateHourVo.getObdHourTransfor().getMileage())).append(",");

                //油耗
                sb.append(integrateHourVo.getOriginalData()[3]).append(",");
                sb.append(integrateHourVo.getObdHourTransfor().getFuel()).append(",");
                sb.append(percentGet(NumberUtils.toFloat(integrateHourVo.getOriginalData()[3], 0), integrateHourVo.getObdHourTransfor().getFuel())).append(",");
                sb.append(integrateHourVo.getOriginalHourVo().getObdHourTransfor().getFuel()).append(",");
                sb.append(percentGet(integrateHourVo.getOriginalHourVo().getObdHourTransfor().getFuel(), integrateHourVo.getObdHourTransfor().getFuel())).append(",");

                //行驶时间
                sb.append(integrateHourVo.getOriginalData()[4]).append(",");
                sb.append(integrateHourVo.getObdHourTransfor().getDuration()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateHourVo.getOriginalData()[4], 0), integrateHourVo.getObdHourTransfor().getDuration())).append(",");
                sb.append(integrateHourVo.getOriginalHourVo().getObdHourTransfor().getDuration()).append(",");
                sb.append(percentGet(integrateHourVo.getOriginalHourVo().getObdHourTransfor().getDuration(), integrateHourVo.getObdHourTransfor().getDuration())).append(",");

                //最大速度
                sb.append(integrateHourVo.getOriginalData()[5]).append(",");
                sb.append(integrateHourVo.getObdHourTransfor().getMaxSpeed()).append(",");
                sb.append(percentGet(NumberUtils.toFloat(integrateHourVo.getOriginalData()[5], 0), integrateHourVo.getObdHourTransfor().getMaxSpeed())).append(",");
                sb.append(integrateHourVo.getOriginalHourVo().getObdHourTransfor().getMaxSpeed()).append(",");
                sb.append(percentGet(integrateHourVo.getOriginalHourVo().getObdHourTransfor().getMaxSpeed(), integrateHourVo.getObdHourTransfor().getMaxSpeed())).append(",");

                //点火次数
                sb.append(integrateHourVo.getOriginalData()[6]).append(",");
                sb.append(integrateHourVo.getAmHourTransfor().getIgnition()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateHourVo.getOriginalData()[6]), integrateHourVo.getAmHourTransfor().getIgnition())).append(",");
                sb.append(integrateHourVo.getOriginalHourVo().getAmHourTransfor().getIgnition()).append(",");
                sb.append(percentGet(integrateHourVo.getOriginalHourVo().getAmHourTransfor().getIgnition(), integrateHourVo.getAmHourTransfor().getIgnition())).append(",");

                //熄火次数
                sb.append(integrateHourVo.getOriginalData()[7]).append(",");
                sb.append(integrateHourVo.getAmHourTransfor().getFlameOut()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateHourVo.getOriginalData()[7]), integrateHourVo.getAmHourTransfor().getFlameOut())).append(",");
                sb.append(integrateHourVo.getOriginalHourVo().getAmHourTransfor().getFlameOut()).append(",");
                sb.append(percentGet(integrateHourVo.getOriginalHourVo().getAmHourTransfor().getFlameOut(), integrateHourVo.getAmHourTransfor().getFlameOut())).append(",");

                //插入次数
                sb.append(integrateHourVo.getOriginalData()[8]).append(",");
                sb.append(integrateHourVo.getAmHourTransfor().getInsertNum()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateHourVo.getOriginalData()[8]), integrateHourVo.getAmHourTransfor().getInsertNum())).append(",");
                sb.append(integrateHourVo.getOriginalHourVo().getAmHourTransfor().getInsertNum()).append(",");
                sb.append(percentGet(integrateHourVo.getOriginalHourVo().getAmHourTransfor().getInsertNum(), integrateHourVo.getAmHourTransfor().getInsertNum())).append(",");

                //急加速次数
                sb.append(integrateHourVo.getOriginalData()[9]).append(",");
                sb.append(integrateHourVo.getDeHourTransfor().getRapidAccelerationCount()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateHourVo.getOriginalData()[9]), integrateHourVo.getDeHourTransfor().getRapidAccelerationCount())).append(",");
                sb.append(integrateHourVo.getOriginalHourVo().getDeHourTransfor().getRapidAccelerationCount()).append(",");
                sb.append(percentGet(integrateHourVo.getOriginalHourVo().getDeHourTransfor().getRapidAccelerationCount(), integrateHourVo.getDeHourTransfor().getRapidAccelerationCount())).append(",");

                //急减速次数
                sb.append(integrateHourVo.getOriginalData()[10]).append(",");
                sb.append(integrateHourVo.getDeHourTransfor().getRapidDecelerationCount()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateHourVo.getOriginalData()[10]), integrateHourVo.getDeHourTransfor().getRapidDecelerationCount())).append(",");
                sb.append(integrateHourVo.getOriginalHourVo().getDeHourTransfor().getRapidDecelerationCount()).append(",");
                sb.append(percentGet(integrateHourVo.getOriginalHourVo().getDeHourTransfor().getRapidDecelerationCount(), integrateHourVo.getDeHourTransfor().getRapidDecelerationCount())).append(",");

                //急转弯次数
                sb.append(integrateHourVo.getOriginalData()[11]).append(",");
                sb.append(integrateHourVo.getDeHourTransfor().getSharpTurnCount()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateHourVo.getOriginalData()[11]), integrateHourVo.getDeHourTransfor().getSharpTurnCount())).append(",");
                sb.append(integrateHourVo.getOriginalHourVo().getDeHourTransfor().getSharpTurnCount()).append(",");
                sb.append(percentGet(integrateHourVo.getOriginalHourVo().getDeHourTransfor().getSharpTurnCount(), integrateHourVo.getDeHourTransfor().getSharpTurnCount())).append(",");

                //连接次数
                sb.append(integrateHourVo.getOriginalData()[12]).append(",");
                sb.append("未知").append(",");
                sb.append("未知").append(",");
                sb.append("未知").append(",");
                sb.append("未知").append(",");
                //更换车机
                sb.append(integrateHourVo.getOriginalData()[13]).append(",");
                sb.append(integrateHourVo.getObdHourTransfor().getDinChange()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateHourVo.getOriginalData()[13]), integrateHourVo.getObdHourTransfor().getDinChange())).append(",");
                sb.append(integrateHourVo.getOriginalHourVo().getObdHourTransfor().getDinChange()).append(",");
                sb.append(percentGet(integrateHourVo.getOriginalHourVo().getObdHourTransfor().getDinChange(), integrateHourVo.getObdHourTransfor().getDinChange())).append(",");

                //最大搜星数
                sb.append(integrateHourVo.getOriginalData()[14]).append(",");
                sb.append(integrateHourVo.getGpsHourTransfor().getMaxSatelliteNum()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateHourVo.getOriginalData()[14]), integrateHourVo.getGpsHourTransfor().getMaxSatelliteNum())).append(",");
                sb.append(integrateHourVo.getOriginalHourVo().getGpsHourTransfor().getMaxSatelliteNum()).append(",");
                sb.append(percentGet(integrateHourVo.getOriginalHourVo().getGpsHourTransfor().getMaxSatelliteNum(), integrateHourVo.getGpsHourTransfor().getMaxSatelliteNum())).append(",");

                //最低电瓶电压
                sb.append(integrateHourVo.getOriginalData()[15]).append(",");
                sb.append(integrateHourVo.getVoltageHourTransfor().getMinVoltage()).append(",");
                sb.append(percentGet(NumberUtils.toFloat(integrateHourVo.getOriginalData()[15]), integrateHourVo.getVoltageHourTransfor().getMinVoltage())).append(",");
                sb.append(integrateHourVo.getOriginalHourVo().getVoltageHourTransfor().getMinVoltage()).append(",");
                sb.append(percentGet(integrateHourVo.getOriginalHourVo().getVoltageHourTransfor().getMinVoltage(), integrateHourVo.getVoltageHourTransfor().getMinVoltage())).append(",");

                //最高电瓶电压
                sb.append(integrateHourVo.getOriginalData()[16]).append(",");
                sb.append(integrateHourVo.getVoltageHourTransfor().getMaxVoltage()).append(",");
                sb.append(percentGet(NumberUtils.toFloat(integrateHourVo.getOriginalData()[16]), integrateHourVo.getVoltageHourTransfor().getMaxVoltage())).append(",");
                sb.append(integrateHourVo.getOriginalHourVo().getVoltageHourTransfor().getMaxVoltage()).append(",");
                sb.append(percentGet(integrateHourVo.getOriginalHourVo().getVoltageHourTransfor().getMaxVoltage(), integrateHourVo.getVoltageHourTransfor().getMaxVoltage())).append(",");

                //是否开车
                sb.append(integrateHourVo.getOriginalData()[17]).append(",");
                sb.append(integrateHourVo.getObdHourTransfor().getIsDrive()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateHourVo.getOriginalData()[17], 0), integrateHourVo.getObdHourTransfor().getIsDrive())).append(",");
                sb.append(integrateHourVo.getOriginalHourVo().getObdHourTransfor().getIsDrive()).append(",");
                sb.append(percentGet(integrateHourVo.getOriginalHourVo().getObdHourTransfor().getIsDrive(), integrateHourVo.getObdHourTransfor().getIsDrive())).append(",");
                //疲劳驾驶
                sb.append(integrateHourVo.getOriginalData()[18]).append(",");
                sb.append(integrateHourVo.getAmHourTransfor().getIsFatigue()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateHourVo.getOriginalData()[18]), integrateHourVo.getAmHourTransfor().getIsFatigue())).append(",");
                sb.append(integrateHourVo.getOriginalHourVo().getAmHourTransfor().getIsFatigue()).append(",");
                sb.append(percentGet(integrateHourVo.getOriginalHourVo().getAmHourTransfor().getIsFatigue(), integrateHourVo.getAmHourTransfor().getIsFatigue())).append(",");

                //高速
                sb.append(integrateHourVo.getOriginalData()[19]).append(",");
                sb.append(integrateHourVo.getObdHourTransfor().getIsHighSpeed()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateHourVo.getOriginalData()[19], 0), integrateHourVo.getObdHourTransfor().getIsHighSpeed())).append(",");
                sb.append(integrateHourVo.getOriginalHourVo().getObdHourTransfor().getIsHighSpeed()).append(",");
                sb.append(percentGet(integrateHourVo.getOriginalHourVo().getObdHourTransfor().getIsHighSpeed(), integrateHourVo.getObdHourTransfor().getIsHighSpeed())).append(",");
                //拔出时长
                sb.append(integrateHourVo.getOriginalData()[20]).append(",");
                sb.append(integrateHourVo.getAmHourTransfor().getPulloutTimes()).append(",");
                sb.append(percentGet(NumberUtils.toFloat(integrateHourVo.getOriginalData()[20]), integrateHourVo.getAmHourTransfor().getPulloutTimes())).append(",");
                sb.append(integrateHourVo.getOriginalHourVo().getAmHourTransfor().getPulloutTimes()).append(",");
                sb.append(percentGet(integrateHourVo.getOriginalHourVo().getAmHourTransfor().getPulloutTimes(), integrateHourVo.getAmHourTransfor().getPulloutTimes())).append(",");

                //在外地
                sb.append(integrateHourVo.getOriginalData()[21]).append(",");
                sb.append(integrateHourVo.getGpsHourTransfor().getIsNonLocal()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateHourVo.getOriginalData()[21]), integrateHourVo.getGpsHourTransfor().getIsNonLocal())).append(",");
                sb.append(integrateHourVo.getOriginalHourVo().getGpsHourTransfor().getIsNonLocal()).append(",");
                sb.append(percentGet(integrateHourVo.getOriginalHourVo().getGpsHourTransfor().getIsNonLocal(), integrateHourVo.getGpsHourTransfor().getIsNonLocal())).append(",");

                //夜间
                sb.append(integrateHourVo.getOriginalData()[22]).append(",");
                sb.append("未知").append(",");
                sb.append("未知").append(",");
                sb.append("未知").append(",");
                sb.append("未知").append(",");
                //单次最大行驶时间
                sb.append(integrateHourVo.getOriginalData()[23]).append(",");
                sb.append("未知").append(",");
                sb.append("未知").append(",");
                sb.append("未知").append(",");
                sb.append("未知").append(",");
                //失联
                sb.append(integrateHourVo.getOriginalData()[24]).append(",");
                sb.append(integrateHourVo.getAmHourTransfor().getIsMissing()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateHourVo.getOriginalData()[24]), integrateHourVo.getAmHourTransfor().getIsMissing())).append(",");
                sb.append(integrateHourVo.getOriginalHourVo().getAmHourTransfor().getIsMissing()).append(",");
                sb.append(percentGet(integrateHourVo.getOriginalHourVo().getAmHourTransfor().getIsMissing(), integrateHourVo.getAmHourTransfor().getIsMissing())).append(",");

                //碰撞告警次数
                sb.append(integrateHourVo.getOriginalData()[25]).append(",");
                sb.append(integrateHourVo.getAmHourTransfor().getCollision()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateHourVo.getOriginalData()[25]), integrateHourVo.getAmHourTransfor().getCollision())).append(",");
                sb.append(integrateHourVo.getOriginalHourVo().getAmHourTransfor().getCollision()).append(",");
                sb.append(percentGet(integrateHourVo.getOriginalHourVo().getAmHourTransfor().getCollision(), integrateHourVo.getAmHourTransfor().getCollision())).append(",");

                //超速告警次数
                sb.append(integrateHourVo.getOriginalData()[26]).append(",");
                sb.append(integrateHourVo.getAmHourTransfor().getOverSpeed()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateHourVo.getOriginalData()[26]), integrateHourVo.getAmHourTransfor().getOverSpeed())).append(",");
                sb.append(integrateHourVo.getOriginalHourVo().getAmHourTransfor().getOverSpeed()).append(",");
                sb.append(percentGet(integrateHourVo.getOriginalHourVo().getAmHourTransfor().getOverSpeed(), integrateHourVo.getAmHourTransfor().getOverSpeed())).append(",");

                //与平台交互次数
                sb.append(integrateHourVo.getOriginalData()[27]).append(",");
                sb.append("未知").append(",");
                sb.append("未知").append(",");
                sb.append("未知").append(",");
                sb.append("未知").append(",");
                //gps上报次数
                sb.append(integrateHourVo.getOriginalData()[28]).append(",");
                sb.append(integrateHourVo.getGpsHourTransfor().getGpsCount()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateHourVo.getOriginalData()[28]), integrateHourVo.getGpsHourTransfor().getGpsCount())).append(",");
                sb.append(integrateHourVo.getOriginalHourVo().getGpsHourTransfor().getGpsCount()).append(",");
                sb.append(percentGet(integrateHourVo.getOriginalHourVo().getGpsHourTransfor().getGpsCount(), integrateHourVo.getGpsHourTransfor().getGpsCount())).append(",\n");

                randomFile.write(sb.toString().getBytes());
            }
        }catch (Exception e) {
            log.error("文件写出异常");
            e.printStackTrace();
        }finally {
            if (randomFile != null) {
                try {
                    randomFile.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    public void writeDayData(List<IntegrateDayVo> list,File file)throws IOException {
        RandomAccessFile randomFile = null;
        try {
            randomFile = new RandomAccessFile(file, "rw");
            randomFile.seek(randomFile.length());
            StringBuffer sb = new StringBuffer();
            sb.append("flag,车的ID,日期,行驶里程,耗油量,行驶时间,最大速度,点火次数,熄火次数,插入次数,急加速次数,急减速次数,急转弯次数,连接次数,是否更换车机,最大搜星数,最低电瓶电压,最高电瓶电压,是否开车,是否疲劳驾驶,是否上了高速,拔出时长,车是否在外地,是否夜间开车,单次最大行驶时间,单次最大形式里程,是否失联,与平台交互次数,当前总里程,当月截止该天的油费,该天上下线次数,当天轨迹条数,删除轨迹条数").append("\n");
            randomFile.write(sb.toString().getBytes());


            for (IntegrateDayVo integrateDayVo : list) {
                sb.setLength(0);
                //old
                if(integrateDayVo.getOriginalData()!=null&&integrateDayVo.getOriginalData().length>0) {
                    sb.append("old").append(",").append(Arrays.stream(integrateDayVo.getOriginalData()).collect(Collectors.joining(","))).append("\n");
                    randomFile.write(sb.toString().getBytes());
                    sb.setLength(0);
                }
                //new
                sb.append("new").append(",");
                sb.append(integrateDayVo.getObdDayTransfor().getCarId()).append(",");
                sb.append(integrateDayVo.getOriginalData()[1]).append(",");
                sb.append(integrateDayVo.getObdDayTransfor().getMileage()).append(",");
                sb.append(integrateDayVo.getObdDayTransfor().getFuel()).append(",");
                sb.append(integrateDayVo.getObdDayTransfor().getDuration()).append(",");
                sb.append(integrateDayVo.getObdDayTransfor().getMaxSpeed()).append(",");
                sb.append(integrateDayVo.getAmDayTransfor().getIgnition()).append(",");
                sb.append(integrateDayVo.getAmDayTransfor().getFlameOut()).append(",");
                sb.append(integrateDayVo.getAmDayTransfor().getInsertNum()).append(",");
                sb.append(integrateDayVo.getDeDayTransfor().getRapidAccelerationCount()).append(",");
                sb.append(integrateDayVo.getDeDayTransfor().getRapidDecelerationCount()).append(",");
                sb.append(integrateDayVo.getDeDayTransfor().getSharpTurnCount()).append(",");

                sb.append("未知").append(",");

                sb.append(integrateDayVo.getObdDayTransfor().getDinChange()).append(",");
                sb.append(integrateDayVo.getGpsDayTransfor().getMaxSatelliteNum()).append(",");
                sb.append(integrateDayVo.getVoltageDayTransfor().getMinVoltage()).append(",");
                sb.append(integrateDayVo.getVoltageDayTransfor().getMaxVoltage()).append(",");
                sb.append(integrateDayVo.getObdDayTransfor().getIsDrive()).append(",");
                sb.append(integrateDayVo.getAmDayTransfor().getIsFatigue()).append(",");
                sb.append(integrateDayVo.getObdDayTransfor().getIsHighSpeed()).append(",");
                sb.append(integrateDayVo.getAmDayTransfor().getPulloutTimes()).append(",");
                sb.append(integrateDayVo.getGpsDayTransfor().getIsNonLocal()).append(",");
                sb.append(integrateDayVo.getObdDayTransfor().getIsNightDrive()).append(",");
                sb.append("未知").append(",");
                sb.append("未知").append(",");
                sb.append(integrateDayVo.getAmDayTransfor().getIsMissing()).append(",");
                sb.append("未知").append(",");
                sb.append(integrateDayVo.getObdDayTransfor().getTotalDistance()).append(",");
                sb.append(integrateDayVo.getObdDayTransfor().getFee()).append(",");

                sb.append("未知").append(",");
                sb.append(integrateDayVo.getTraceDayTransfor().getTraceCounts()).append(",");
                sb.append(integrateDayVo.getTraceDeleteDayTransfor().getTraceDeleteCounts()).append(",\n");
                randomFile.write(sb.toString().getBytes());


            }
        }catch (Exception e) {
            log.error("文件写出异常");
            e.printStackTrace();
        }finally {
            if (randomFile != null) {
                try {
                    randomFile.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void writeDayDataWithPer(List<IntegrateDayVo> list,File file)throws IOException {
        RandomAccessFile randomFile = null;
        try {
            randomFile = new RandomAccessFile(file, "rw");
            randomFile.seek(randomFile.length());
            StringBuffer sb = new StringBuffer();
            sb.append("车的ID,日期,old行驶里程,new行驶里程,old_per,old耗油量,new耗油量,old_per,old行驶时间,new行驶时间,old_per,old最大速度,new最大速度,old_per,old点火次数,new点火次数,old_per,old熄火次数,new熄火次数,old_per,old插入次数,new插入次数,old_per,old急加速次数,new急加速次数,old_per,old急减速次数,new急减速次数,old_per,old急转弯次数,new急转弯次数,old_per,old连接次数,new连接次数,old_per,old是否更换车机,new是否更换车机,old_per,old最大搜星数,new最大搜星数,old_per,old最低电瓶电压,new最低电瓶电压,old_per,old最高电瓶电压,new最高电瓶电压,old_per,old是否开车,new是否开车,old_per,old是否疲劳驾驶,new是否疲劳驾驶,old_per,old是否上了高速,new是否上了高速,old_per,old拔出时长,new拔出时长,old_per,old车是否在外地,new车是否在外地,old_per,old是否夜间开车,new是否夜间开车,old_per,old单次最大行驶时间,new单次最大行驶时间,old_per,old单次最大行驶里程,new单次最大行驶里程,old_per,old是否失联,new是否失联,old_per,old与平台交互次数,new与平台交互次数,old_per,old当前总里程,new当前总里程,old_per,old当月截止该天的油费,new当月截止该天的油费,old_per,old该天上下线次数,new该天上下线次数,old_per,old当天轨迹条数,new当天轨迹条数,old_per,old删除轨迹条数,new删除轨迹条数,old_per").append("\n");
            randomFile.write(sb.toString().getBytes());


            for (IntegrateDayVo integrateDayVo : list) {
                sb.setLength(0);

                //new
                sb.append(integrateDayVo.getOriginalData()[0]).append(",");
                sb.append(integrateDayVo.getOriginalData()[1]).append(",");

                //里程
                sb.append(integrateDayVo.getOriginalData()[2]).append(",");
                sb.append(integrateDayVo.getObdDayTransfor().getMileage()).append(",");
                sb.append(percentGet(NumberUtils.toFloat(integrateDayVo.getOriginalData()[2],0),integrateDayVo.getObdDayTransfor().getMileage())).append(",");


                sb.append(integrateDayVo.getOriginalData()[3]).append(",");
                sb.append(integrateDayVo.getObdDayTransfor().getFuel()).append(",");
                sb.append(percentGet(NumberUtils.toFloat(integrateDayVo.getOriginalData()[3],0),integrateDayVo.getObdDayTransfor().getFuel())).append(",");

                sb.append(integrateDayVo.getOriginalData()[4]).append(",");
                sb.append(integrateDayVo.getObdDayTransfor().getDuration()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateDayVo.getOriginalData()[4],0),integrateDayVo.getObdDayTransfor().getDuration())).append(",");

                sb.append(integrateDayVo.getOriginalData()[5]).append(",");
                sb.append(integrateDayVo.getObdDayTransfor().getMaxSpeed()).append(",");
                sb.append(percentGet(NumberUtils.toFloat(integrateDayVo.getOriginalData()[5],0),integrateDayVo.getObdDayTransfor().getMaxSpeed())).append(",");

                sb.append(integrateDayVo.getOriginalData()[6]).append(",");
                sb.append(integrateDayVo.getAmDayTransfor().getIgnition()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateDayVo.getOriginalData()[6],0),integrateDayVo.getAmDayTransfor().getIgnition())).append(",");

                sb.append(integrateDayVo.getOriginalData()[7]).append(",");
                sb.append(integrateDayVo.getAmDayTransfor().getFlameOut()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateDayVo.getOriginalData()[7],0),integrateDayVo.getAmDayTransfor().getFlameOut())).append(",");

                sb.append(integrateDayVo.getOriginalData()[8]).append(",");
                sb.append(integrateDayVo.getAmDayTransfor().getInsertNum()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateDayVo.getOriginalData()[8],0),integrateDayVo.getAmDayTransfor().getInsertNum())).append(",");


                sb.append(integrateDayVo.getOriginalData()[9]).append(",");
                sb.append(integrateDayVo.getDeDayTransfor().getRapidAccelerationCount()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateDayVo.getOriginalData()[9],0),integrateDayVo.getDeDayTransfor().getRapidAccelerationCount())).append(",");

                sb.append(integrateDayVo.getOriginalData()[10]).append(",");
                sb.append(integrateDayVo.getDeDayTransfor().getRapidDecelerationCount()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateDayVo.getOriginalData()[10],0),integrateDayVo.getDeDayTransfor().getRapidDecelerationCount())).append(",");

                sb.append(integrateDayVo.getOriginalData()[11]).append(",");
                sb.append(integrateDayVo.getDeDayTransfor().getSharpTurnCount()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateDayVo.getOriginalData()[11],0),integrateDayVo.getDeDayTransfor().getSharpTurnCount())).append(",");


                sb.append(integrateDayVo.getOriginalData()[12]).append(",");
                sb.append("未知").append(",");
                sb.append("未知").append(",");

                sb.append(integrateDayVo.getOriginalData()[13]).append(",");
                sb.append(integrateDayVo.getObdDayTransfor().getDinChange()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateDayVo.getOriginalData()[13],0),integrateDayVo.getObdDayTransfor().getDinChange())).append(",");

                sb.append(integrateDayVo.getOriginalData()[14]).append(",");
                sb.append(integrateDayVo.getGpsDayTransfor().getMaxSatelliteNum()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateDayVo.getOriginalData()[14],0),integrateDayVo.getGpsDayTransfor().getMaxSatelliteNum())).append(",");

                sb.append(integrateDayVo.getOriginalData()[15]).append(",");
                sb.append(integrateDayVo.getVoltageDayTransfor().getMinVoltage()).append(",");
                sb.append(percentGet(NumberUtils.toFloat(integrateDayVo.getOriginalData()[15],0),integrateDayVo.getVoltageDayTransfor().getMinVoltage())).append(",");

                sb.append(integrateDayVo.getOriginalData()[16]).append(",");
                sb.append(integrateDayVo.getVoltageDayTransfor().getMaxVoltage()).append(",");
                sb.append(percentGet(NumberUtils.toFloat(integrateDayVo.getOriginalData()[16],0),integrateDayVo.getVoltageDayTransfor().getMaxVoltage())).append(",");

                sb.append(integrateDayVo.getOriginalData()[17]).append(",");
                sb.append(integrateDayVo.getObdDayTransfor().getIsDrive()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateDayVo.getOriginalData()[17],0),integrateDayVo.getObdDayTransfor().getIsDrive())).append(",");

                sb.append(integrateDayVo.getOriginalData()[18]).append(",");
                sb.append(integrateDayVo.getAmDayTransfor().getIsFatigue()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateDayVo.getOriginalData()[18],0),integrateDayVo.getAmDayTransfor().getIsFatigue())).append(",");

                sb.append(integrateDayVo.getOriginalData()[19]).append(",");
                sb.append(integrateDayVo.getObdDayTransfor().getIsHighSpeed()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateDayVo.getOriginalData()[19],0),integrateDayVo.getObdDayTransfor().getIsHighSpeed())).append(",");

                sb.append(integrateDayVo.getOriginalData()[20]).append(",");
                sb.append(integrateDayVo.getAmDayTransfor().getPulloutTimes()).append(",");
                sb.append(percentGet(NumberUtils.toFloat(integrateDayVo.getOriginalData()[20],0),integrateDayVo.getAmDayTransfor().getPulloutTimes())).append(",");

                sb.append(integrateDayVo.getOriginalData()[21]).append(",");
                sb.append(integrateDayVo.getGpsDayTransfor().getIsNonLocal()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateDayVo.getOriginalData()[21],0),integrateDayVo.getGpsDayTransfor().getIsNonLocal())).append(",");

                sb.append(integrateDayVo.getOriginalData()[22]).append(",");
                sb.append(integrateDayVo.getObdDayTransfor().getIsNightDrive()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateDayVo.getOriginalData()[22],0),integrateDayVo.getObdDayTransfor().getIsNightDrive())).append(",");

                sb.append(integrateDayVo.getOriginalData()[23]).append(",");
                sb.append("未知").append(",");
                sb.append("未知").append(",");

                sb.append(integrateDayVo.getOriginalData()[24]).append(",");
                sb.append("未知").append(",");
                sb.append("未知").append(",");

                sb.append(integrateDayVo.getOriginalData()[25]).append(",");
                sb.append(integrateDayVo.getAmDayTransfor().getIsMissing()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateDayVo.getOriginalData()[25],0),integrateDayVo.getAmDayTransfor().getIsMissing())).append(",");

                sb.append(integrateDayVo.getOriginalData()[26]).append(",");
                sb.append("未知").append(",");
                sb.append("未知").append(",");

                sb.append(integrateDayVo.getOriginalData()[27]).append(",");
                sb.append(integrateDayVo.getObdDayTransfor().getTotalDistance()).append(",");
                sb.append(percentGet(NumberUtils.toFloat(integrateDayVo.getOriginalData()[27],0),integrateDayVo.getObdDayTransfor().getTotalDistance())).append(",");

                sb.append(integrateDayVo.getOriginalData()[28]).append(",");
                sb.append(integrateDayVo.getObdDayTransfor().getFee()).append(",");
                sb.append(percentGet(NumberUtils.toFloat(integrateDayVo.getOriginalData()[28],0),integrateDayVo.getObdDayTransfor().getFee())).append(",");

                sb.append(integrateDayVo.getOriginalData()[29]).append(",");
                sb.append("未知").append(",");
                sb.append("未知").append(",");

                sb.append(integrateDayVo.getOriginalData()[30]).append(",");
                sb.append(integrateDayVo.getTraceDayTransfor().getTraceCounts()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateDayVo.getOriginalData()[30],0),integrateDayVo.getTraceDayTransfor().getTraceCounts())).append(",");

                sb.append(integrateDayVo.getOriginalData()[31]).append(",");
                sb.append(integrateDayVo.getTraceDeleteDayTransfor().getTraceDeleteCounts()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateDayVo.getOriginalData()[31],0),integrateDayVo.getTraceDeleteDayTransfor().getTraceDeleteCounts())).append(",\n");

                randomFile.write(sb.toString().getBytes());

            }
        }catch (Exception e) {
            log.error("文件写出异常");
            e.printStackTrace();
        }finally {
            if (randomFile != null) {
                try {
                    randomFile.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    public void writeDayDataWithPerAndOriginal(List<IntegrateDayVo> list,File file)throws IOException {
        RandomAccessFile randomFile = null;
        try {
            randomFile = new RandomAccessFile(file, "rw");
            randomFile.seek(randomFile.length());
            StringBuffer sb = new StringBuffer();
            sb.append("车的ID,日期,old行驶里程,new行驶里程,old_per,original行驶里程,new_per,old耗油量,new耗油量,old_per,original耗油量,new_per,old行驶时间,new行驶时间,old_per,original行驶时间,new_per,old最大速度,new最大速度,old_per,original最大速度,new_per,old点火次数,new点火次数,old_per,original点火次数,new_per,old熄火次数,new熄火次数,old_per,original熄火次数,new_per,old插入次数,new插入次数,old_per,original插入次数,new_per,old急加速次数,new急加速次数,old_per,original急加速次数,new_per,old急减速次数,new急减速次数,old_per,original急减速次数,new_per,old急转弯次数,new急转弯次数,old_per,original急转弯次数,new_per,old连接次数,new连接次数,old_per,original连接次数,new_per,old是否更换车机,new是否更换车机,old_per,original是否更换车机,new_per,old最大搜星数,new最大搜星数,old_per,original最大搜星数,new_per,old最低电瓶电压,new最低电瓶电压,old_per,original最低电瓶电压,new_per,old最高电瓶电压,new最高电瓶电压,old_per,original最高电瓶电压,new_per,old是否开车,new是否开车,old_per,original是否开车,new_per,old是否疲劳驾驶,new是否疲劳驾驶,old_per,original是否疲劳驾驶,new_per,old是否上了高速,new是否上了高速,old_per,original是否上了高速,new_per,old拔出时长,new拔出时长,old_per,original拔出时长,new_per,old车是否在外地,new车是否在外地,old_per,original车是否在外地,new_per,old是否夜间开车,new是否夜间开车,old_per,original是否夜间开车,new_per,old单次最大行驶时间,new单次最大行驶时间,old_per,original单次最大行驶时间,new_per,old单次最大行驶里程,new单次最大行驶里程,old_per,original单次最大行驶里程,new_per,old是否失联,new是否失联,old_per,original是否失联,new_per,old与平台交互次数,new与平台交互次数,old_per,original与平台交互次数,new_per,old当前总里程,new当前总里程,old_per,original当前总里程,new_per,old当月截止该天的油费,new当月截止该天的油费,old_per,original当月截止该天的油费,new_per,old该天上下线次数,new该天上下线次数,old_per,original该天上下线次数,new_per,old当天轨迹条数,new当天轨迹条数,old_per,original当天轨迹条数,new_per,old删除轨迹条数,new删除轨迹条数,old_per,original删除轨迹条数,new_per").append("\n");
            randomFile.write(sb.toString().getBytes());


            for (IntegrateDayVo integrateDayVo : list) {

                log.info("integrateDayVo vo  is {}", integrateDayVo);
                if (integrateDayVo.getIntegrateDayVo() == null ) {
                    log.info("integrateDayVo is {}");
                    continue;
                }
                if( integrateDayVo.getIntegrateDayVo().getObdDayTransfor() == null){
                    continue;
                }

                sb.setLength(0);
                //new
                sb.append(integrateDayVo.getOriginalData()[0]).append(",");
                sb.append(integrateDayVo.getOriginalData()[1]).append(",");

                //里程
                sb.append(integrateDayVo.getOriginalData()[2]).append(",");
                sb.append(integrateDayVo.getObdDayTransfor().getMileage()).append(",");
                sb.append(percentGet(NumberUtils.toFloat(integrateDayVo.getOriginalData()[2],0),integrateDayVo.getObdDayTransfor().getMileage())).append(",");
                sb.append(integrateDayVo.getIntegrateDayVo().getObdDayTransfor().getMileage()).append(",");
                sb.append(percentGet(integrateDayVo.getIntegrateDayVo().getObdDayTransfor().getMileage(),integrateDayVo.getObdDayTransfor().getMileage())).append(",");


                sb.append(integrateDayVo.getOriginalData()[3]).append(",");
                sb.append(integrateDayVo.getObdDayTransfor().getFuel()).append(",");
                sb.append(percentGet(NumberUtils.toFloat(integrateDayVo.getOriginalData()[3],0),integrateDayVo.getObdDayTransfor().getFuel())).append(",");
                sb.append(integrateDayVo.getIntegrateDayVo().getObdDayTransfor().getFuel()).append(",");
                sb.append(percentGet(integrateDayVo.getIntegrateDayVo().getObdDayTransfor().getFuel(),integrateDayVo.getObdDayTransfor().getFuel())).append(",");


                sb.append(integrateDayVo.getOriginalData()[4]).append(",");
                sb.append(integrateDayVo.getObdDayTransfor().getDuration()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateDayVo.getOriginalData()[4],0),integrateDayVo.getObdDayTransfor().getDuration())).append(",");
                sb.append(integrateDayVo.getIntegrateDayVo().getObdDayTransfor().getDuration()).append(",");
                sb.append(percentGet(integrateDayVo.getIntegrateDayVo().getObdDayTransfor().getDuration(),integrateDayVo.getObdDayTransfor().getDuration())).append(",");


                sb.append(integrateDayVo.getOriginalData()[5]).append(",");
                sb.append(integrateDayVo.getObdDayTransfor().getMaxSpeed()).append(",");
                sb.append(percentGet(NumberUtils.toFloat(integrateDayVo.getOriginalData()[5],0),integrateDayVo.getObdDayTransfor().getMaxSpeed())).append(",");
                sb.append(integrateDayVo.getIntegrateDayVo().getObdDayTransfor().getMaxSpeed()).append(",");
                sb.append(percentGet(integrateDayVo.getIntegrateDayVo().getObdDayTransfor().getMaxSpeed(),integrateDayVo.getObdDayTransfor().getMaxSpeed())).append(",");



                sb.append(integrateDayVo.getOriginalData()[6]).append(",");
                sb.append(integrateDayVo.getAmDayTransfor().getIgnition()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateDayVo.getOriginalData()[6],0),integrateDayVo.getAmDayTransfor().getIgnition())).append(",");
                sb.append(integrateDayVo.getIntegrateDayVo().getAmDayTransfor().getIgnition()).append(",");
                sb.append(percentGet(integrateDayVo.getIntegrateDayVo().getAmDayTransfor().getIgnition(),integrateDayVo.getAmDayTransfor().getIgnition())).append(",");



                sb.append(integrateDayVo.getOriginalData()[7]).append(",");
                sb.append(integrateDayVo.getAmDayTransfor().getFlameOut()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateDayVo.getOriginalData()[7],0),integrateDayVo.getAmDayTransfor().getFlameOut())).append(",");
                sb.append(integrateDayVo.getIntegrateDayVo().getAmDayTransfor().getFlameOut()).append(",");
                sb.append(percentGet(integrateDayVo.getIntegrateDayVo().getAmDayTransfor().getFlameOut(),integrateDayVo.getAmDayTransfor().getFlameOut())).append(",");


                sb.append(integrateDayVo.getOriginalData()[8]).append(",");
                sb.append(integrateDayVo.getAmDayTransfor().getInsertNum()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateDayVo.getOriginalData()[8],0),integrateDayVo.getAmDayTransfor().getInsertNum())).append(",");
                sb.append(integrateDayVo.getIntegrateDayVo().getAmDayTransfor().getInsertNum()).append(",");
                sb.append(percentGet(integrateDayVo.getIntegrateDayVo().getAmDayTransfor().getInsertNum(),integrateDayVo.getAmDayTransfor().getInsertNum())).append(",");


                sb.append(integrateDayVo.getOriginalData()[9]).append(",");
                sb.append(integrateDayVo.getDeDayTransfor().getRapidAccelerationCount()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateDayVo.getOriginalData()[9],0),integrateDayVo.getDeDayTransfor().getRapidAccelerationCount())).append(",");
                sb.append(integrateDayVo.getIntegrateDayVo().getAmDayTransfor().getInsertNum()).append(",");
                sb.append(percentGet(integrateDayVo.getIntegrateDayVo().getDeDayTransfor().getRapidAccelerationCount(),integrateDayVo.getDeDayTransfor().getRapidAccelerationCount())).append(",");


                sb.append(integrateDayVo.getOriginalData()[10]).append(",");
                sb.append(integrateDayVo.getDeDayTransfor().getRapidDecelerationCount()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateDayVo.getOriginalData()[10],0),integrateDayVo.getDeDayTransfor().getRapidDecelerationCount())).append(",");
                sb.append(integrateDayVo.getIntegrateDayVo().getDeDayTransfor().getRapidDecelerationCount()).append(",");
                sb.append(percentGet(integrateDayVo.getIntegrateDayVo().getDeDayTransfor().getRapidDecelerationCount(),integrateDayVo.getDeDayTransfor().getRapidDecelerationCount())).append(",");


                sb.append(integrateDayVo.getOriginalData()[11]).append(",");
                sb.append(integrateDayVo.getDeDayTransfor().getSharpTurnCount()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateDayVo.getOriginalData()[11],0),integrateDayVo.getDeDayTransfor().getSharpTurnCount())).append(",");
                sb.append(integrateDayVo.getIntegrateDayVo().getDeDayTransfor().getSharpTurnCount()).append(",");
                sb.append(percentGet(integrateDayVo.getIntegrateDayVo().getDeDayTransfor().getSharpTurnCount(),integrateDayVo.getDeDayTransfor().getSharpTurnCount())).append(",");



                sb.append(integrateDayVo.getOriginalData()[12]).append(",");
                sb.append("未知").append(",");
                sb.append("未知").append(",");
                sb.append("未知").append(",");
                sb.append("未知").append(",");


                sb.append(integrateDayVo.getOriginalData()[13]).append(",");
                sb.append(integrateDayVo.getObdDayTransfor().getDinChange()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateDayVo.getOriginalData()[13],0),integrateDayVo.getObdDayTransfor().getDinChange())).append(",");
                sb.append(integrateDayVo.getIntegrateDayVo().getObdDayTransfor().getDinChange()).append(",");
                sb.append(percentGet(integrateDayVo.getIntegrateDayVo().getObdDayTransfor().getDinChange(),integrateDayVo.getObdDayTransfor().getDinChange())).append(",");


                sb.append(integrateDayVo.getOriginalData()[14]).append(",");
                sb.append(integrateDayVo.getGpsDayTransfor().getMaxSatelliteNum()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateDayVo.getOriginalData()[14],0),integrateDayVo.getGpsDayTransfor().getMaxSatelliteNum())).append(",");
                sb.append(integrateDayVo.getIntegrateDayVo().getGpsDayTransfor().getMaxSatelliteNum()).append(",");
                sb.append(percentGet(integrateDayVo.getIntegrateDayVo().getGpsDayTransfor().getMaxSatelliteNum(),integrateDayVo.getGpsDayTransfor().getMaxSatelliteNum())).append(",");


                sb.append(integrateDayVo.getOriginalData()[15]).append(",");
                sb.append(integrateDayVo.getVoltageDayTransfor().getMinVoltage()).append(",");
                sb.append(percentGet(NumberUtils.toFloat(integrateDayVo.getOriginalData()[15],0),integrateDayVo.getVoltageDayTransfor().getMinVoltage())).append(",");
                sb.append(integrateDayVo.getIntegrateDayVo().getVoltageDayTransfor().getMinVoltage()).append(",");
                sb.append(percentGet(integrateDayVo.getIntegrateDayVo().getVoltageDayTransfor().getMinVoltage(),integrateDayVo.getVoltageDayTransfor().getMinVoltage())).append(",");


                sb.append(integrateDayVo.getOriginalData()[16]).append(",");
                sb.append(integrateDayVo.getVoltageDayTransfor().getMaxVoltage()).append(",");
                sb.append(percentGet(NumberUtils.toFloat(integrateDayVo.getOriginalData()[16],0),integrateDayVo.getVoltageDayTransfor().getMaxVoltage())).append(",");
                sb.append(integrateDayVo.getIntegrateDayVo().getVoltageDayTransfor().getMaxVoltage()).append(",");
                sb.append(percentGet(integrateDayVo.getIntegrateDayVo().getVoltageDayTransfor().getMaxVoltage(),integrateDayVo.getVoltageDayTransfor().getMaxVoltage())).append(",");


                sb.append(integrateDayVo.getOriginalData()[17]).append(",");
                sb.append(integrateDayVo.getObdDayTransfor().getIsDrive()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateDayVo.getOriginalData()[17],0),integrateDayVo.getObdDayTransfor().getIsDrive())).append(",");
                sb.append(integrateDayVo.getIntegrateDayVo().getObdDayTransfor().getIsDrive()).append(",");
                sb.append(percentGet(integrateDayVo.getIntegrateDayVo().getObdDayTransfor().getIsDrive(),integrateDayVo.getObdDayTransfor().getIsDrive())).append(",");


                sb.append(integrateDayVo.getOriginalData()[18]).append(",");
                sb.append(integrateDayVo.getAmDayTransfor().getIsFatigue()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateDayVo.getOriginalData()[18],0),integrateDayVo.getAmDayTransfor().getIsFatigue())).append(",");
                sb.append(integrateDayVo.getIntegrateDayVo().getAmDayTransfor().getIsFatigue()).append(",");
                sb.append(percentGet(integrateDayVo.getIntegrateDayVo().getAmDayTransfor().getIsFatigue(),integrateDayVo.getAmDayTransfor().getIsFatigue())).append(",");



                sb.append(integrateDayVo.getOriginalData()[19]).append(",");
                sb.append(integrateDayVo.getObdDayTransfor().getIsHighSpeed()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateDayVo.getOriginalData()[19],0),integrateDayVo.getObdDayTransfor().getIsHighSpeed())).append(",");
                sb.append(integrateDayVo.getIntegrateDayVo().getObdDayTransfor().getIsHighSpeed()).append(",");
                sb.append(percentGet(integrateDayVo.getIntegrateDayVo().getObdDayTransfor().getIsHighSpeed(),integrateDayVo.getObdDayTransfor().getIsHighSpeed())).append(",");


                sb.append(integrateDayVo.getOriginalData()[20]).append(",");
                sb.append(integrateDayVo.getAmDayTransfor().getPulloutTimes()).append(",");
                sb.append(percentGet(NumberUtils.toFloat(integrateDayVo.getOriginalData()[20],0),integrateDayVo.getAmDayTransfor().getPulloutTimes())).append(",");
                sb.append(integrateDayVo.getIntegrateDayVo().getAmDayTransfor().getPulloutTimes()).append(",");
                sb.append(percentGet(integrateDayVo.getIntegrateDayVo().getAmDayTransfor().getPulloutTimes(),integrateDayVo.getAmDayTransfor().getPulloutTimes())).append(",");


                sb.append(integrateDayVo.getOriginalData()[21]).append(",");
                sb.append(integrateDayVo.getGpsDayTransfor().getIsNonLocal()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateDayVo.getOriginalData()[21],0),integrateDayVo.getGpsDayTransfor().getIsNonLocal())).append(",");
                sb.append(integrateDayVo.getIntegrateDayVo().getGpsDayTransfor().getIsNonLocal()).append(",");
                sb.append(percentGet(integrateDayVo.getIntegrateDayVo().getGpsDayTransfor().getIsNonLocal(),integrateDayVo.getGpsDayTransfor().getIsNonLocal())).append(",");


                sb.append(integrateDayVo.getOriginalData()[22]).append(",");
                sb.append(integrateDayVo.getObdDayTransfor().getIsNightDrive()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateDayVo.getOriginalData()[22],0),integrateDayVo.getObdDayTransfor().getIsNightDrive())).append(",");
                sb.append(integrateDayVo.getIntegrateDayVo().getObdDayTransfor().getIsNightDrive()).append(",");
                sb.append(percentGet(integrateDayVo.getIntegrateDayVo().getObdDayTransfor().getIsNightDrive(),integrateDayVo.getObdDayTransfor().getIsNightDrive())).append(",");


                sb.append(integrateDayVo.getOriginalData()[23]).append(",");
                sb.append("未知").append(",");
                sb.append("未知").append(",");
                sb.append("未知").append(",");
                sb.append("未知").append(",");

                sb.append(integrateDayVo.getOriginalData()[24]).append(",");
                sb.append("未知").append(",");
                sb.append("未知").append(",");
                sb.append("未知").append(",");
                sb.append("未知").append(",");

                sb.append(integrateDayVo.getOriginalData()[25]).append(",");
                sb.append(integrateDayVo.getAmDayTransfor().getIsMissing()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateDayVo.getOriginalData()[25],0),integrateDayVo.getAmDayTransfor().getIsMissing())).append(",");
                sb.append(integrateDayVo.getIntegrateDayVo().getAmDayTransfor().getIsMissing()).append(",");
                sb.append(percentGet(integrateDayVo.getIntegrateDayVo().getAmDayTransfor().getIsMissing(),integrateDayVo.getAmDayTransfor().getIsMissing())).append(",");


                sb.append(integrateDayVo.getOriginalData()[26]).append(",");
                sb.append("未知").append(",");
                sb.append("未知").append(",");
                sb.append("未知").append(",");
                sb.append("未知").append(",");

                sb.append(integrateDayVo.getOriginalData()[27]).append(",");
                sb.append(integrateDayVo.getObdDayTransfor().getTotalDistance()).append(",");
                sb.append(percentGet(NumberUtils.toFloat(integrateDayVo.getOriginalData()[27],0),integrateDayVo.getObdDayTransfor().getTotalDistance())).append(",");
                sb.append(integrateDayVo.getIntegrateDayVo().getObdDayTransfor().getTotalDistance()).append(",");
                sb.append(percentGet(integrateDayVo.getIntegrateDayVo().getObdDayTransfor().getTotalDistance(),integrateDayVo.getObdDayTransfor().getTotalDistance())).append(",");


                sb.append(integrateDayVo.getOriginalData()[28]).append(",");
                sb.append(integrateDayVo.getObdDayTransfor().getFee()).append(",");
                sb.append(percentGet(NumberUtils.toFloat(integrateDayVo.getOriginalData()[28],0),integrateDayVo.getObdDayTransfor().getFee())).append(",");
                sb.append(integrateDayVo.getIntegrateDayVo().getObdDayTransfor().getFee()).append(",");
                sb.append(percentGet(integrateDayVo.getIntegrateDayVo().getObdDayTransfor().getFee(),integrateDayVo.getObdDayTransfor().getFee())).append(",");


                sb.append(integrateDayVo.getOriginalData()[29]).append(",");
                sb.append("未知").append(",");
                sb.append("未知").append(",");
                sb.append("未知").append(",");
                sb.append("未知").append(",");

                sb.append(integrateDayVo.getOriginalData()[30]).append(",");
                sb.append(integrateDayVo.getTraceDayTransfor().getTraceCounts()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateDayVo.getOriginalData()[30],0),integrateDayVo.getTraceDayTransfor().getTraceCounts())).append(",");
                sb.append(integrateDayVo.getIntegrateDayVo().getTraceDayTransfor().getTraceCounts()).append(",");
                sb.append(percentGet(integrateDayVo.getIntegrateDayVo().getTraceDayTransfor().getTraceCounts(),integrateDayVo.getTraceDayTransfor().getTraceCounts())).append(",");


                sb.append(integrateDayVo.getOriginalData()[31]).append(",");
                sb.append(integrateDayVo.getTraceDeleteDayTransfor().getTraceDeleteCounts()).append(",");
                sb.append(percentGet(NumberUtils.toDouble(integrateDayVo.getOriginalData()[31],0),integrateDayVo.getTraceDeleteDayTransfor().getTraceDeleteCounts())).append(",");
                sb.append(integrateDayVo.getIntegrateDayVo().getTraceDeleteDayTransfor().getTraceDeleteCounts()).append(",");
                sb.append(percentGet(integrateDayVo.getIntegrateDayVo().getTraceDeleteDayTransfor().getTraceDeleteCounts(),integrateDayVo.getTraceDeleteDayTransfor().getTraceDeleteCounts())).append("\n");


                randomFile.write(sb.toString().getBytes());

            }
        }catch (Exception e) {
            log.error("文件写出异常");
            e.printStackTrace();
        }finally {
            if (randomFile != null) {
                try {
                    randomFile.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public List<IntegrateHourVo> getHourDatasFromFile(boolean flag,MultipartFile file, String originalName,String findDate) {

        String dateTimeFormat = "%s %s%s";

        File dest = new File(fileProperties.getOutPath(),originalName );
        try {
            file.transferTo(dest);
        } catch (IOException e) {
            log.error("exception:",e);
        }
        List<IntegrateHourVo> list = new LinkedList();
        try (Stream<String> stream = Files.lines(Paths.get(dest.toURI()))){
            stream.map(s->s.split(",")).forEach(strs->{
                String time;
                if(NumberUtils.toInt(strs[1])>9)
                    time = String.format(dateTimeFormat, findDate, "", strs[1]);
                else
                    time = String.format(dateTimeFormat, findDate, "0", strs[1]);
                long times = 0;
                try {
                    times = DateTimeUtil.strToTimestamp(time, "yyyy-MM-dd HH");
                    log.info("str is {},and times is {}", time, times);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                list.add(getHourFromCarId(flag, times, strs[0], strs));
            });
        } catch (IOException e) {
            log.error("data error",e);
        }
        return list;
    }

    public List<IntegrateHourVo> getHourDatasFromFileWithOriginal(boolean flag,MultipartFile file, String originalName,String findDate) {


        String dateTimeFormat = "%s %s%s";

        File dest = new File(fileProperties.getOutPath(),originalName );
        try {
            file.transferTo(dest);
        } catch (IOException e) {
            log.error("exception:",e);
        }
        List<IntegrateHourVo> list = new LinkedList();
        try (Stream<String> stream = Files.lines(Paths.get(dest.toURI()))){
            stream.map(s->s.split(",")).forEach(strs->{
                String time;
                String dateTimeFrom;
                String dateTimeTo;
                if (NumberUtils.toInt(strs[1]) > 9) {
                    time = String.format(dateTimeFormat, findDate, "", strs[1]);
                    dateTimeFrom=String.format(DATE_FROM, findDate, "", strs[1]);
                    dateTimeTo=String.format(DATE_TO, findDate, "", strs[1]);
                } else {
                    time = String.format(dateTimeFormat, findDate, "0", strs[1]);
                    dateTimeFrom=String.format(DATE_FROM, findDate, "0", strs[1]);
                    dateTimeTo=String.format(DATE_TO, findDate, "0", strs[1]);
                }
                long times = 0;
                try {
                    times = DateTimeUtil.strToTimestamp(time, "yyyy-MM-dd HH");
                    log.debug("str is {},and times is {}", time, times);
                } catch (ParseException e) {
                    log.error("time format error",e);
                }
                IntegrateHourVo integrateHourVo = getHourFromCarId(flag, times, strs[0], strs);

                log.debug("from bigdao i found {}, data from {},date to {}",strs[0], dateTimeFrom, dateTimeTo);

                IntegrateHourVo nextVo = cstStramDataValidateIntegratedService.createIntegrate(strs[0], dateTimeFrom, dateTimeTo);

                integrateHourVo.setOriginalHourVo(nextVo);
                list.add(integrateHourVo);
                });
        } catch (IOException e) {
            log.error("data error",e);
        }
        return list;
    }

    public  IntegrateHourVo getHourFromCarId(boolean flag,Long datetime,String carId,String[] strs){
        try {

            log.debug("carId:{},times:{}",carId,datetime.longValue());


            DeHourTransfor deHourTransfor=(DeHourTransfor)cstStreamHourIntegrateService.
                    getHourTransfor(carId,datetime, HBaseTable.HOUR_STATISTICS.getFourthFamilyName(),
                    HbaseColumn.HourStatisticsCloumn.deHourColumns, DeHourTransfor.class ).getData();
            ObdHourTransfor obdHourTransfor=(ObdHourTransfor)cstStreamHourIntegrateService.getHourTransfor(carId,datetime,
                    HBaseTable.HOUR_STATISTICS.getFirstFamilyName(), HbaseColumn.HourStatisticsCloumn.obdHourColumns, ObdHourTransfor.class).getData();
            GpsHourTransfor gpsHourTransfor=(GpsHourTransfor)cstStreamHourIntegrateService.getHourTransfor(carId, datetime,
                    HBaseTable.HOUR_STATISTICS.getSecondFamilyName(), HbaseColumn.HourStatisticsCloumn.gpsHourColumns, GpsHourTransfor.class).getData();
            AmHourTransfor amHourTransfor=(AmHourTransfor)cstStreamHourIntegrateService.getHourTransfor(carId, datetime,
                    HBaseTable.HOUR_STATISTICS.getThirdFamilyName(), HbaseColumn.HourStatisticsCloumn.amHourColumns, AmHourTransfor.class).getData();
            TraceHourTransfor traceHourTransfor=(TraceHourTransfor)cstStreamHourIntegrateService.getHourTransfor(carId, datetime,
                    HBaseTable.HOUR_STATISTICS.getFifthFamilyName(), HbaseColumn.HourStatisticsCloumn.traceHourColumns, TraceHourTransfor.class).getData();
            TraceDeleteHourTransfor traceDeleteHourTransfor=(TraceDeleteHourTransfor)cstStreamHourIntegrateService.getHourTransfor(strs[0],datetime,
                    HBaseTable.HOUR_STATISTICS.getSixthFamilyName(), HbaseColumn.HourStatisticsCloumn.traceDeleteHourColumns, TraceDeleteHourTransfor.class).getData();
            VoltageHourTransfor voltageHourTransfor=(VoltageHourTransfor)cstStreamHourIntegrateService.getHourTransfor(carId,datetime,
                    HBaseTable.HOUR_STATISTICS.getSeventhFamilyName(), HbaseColumn.HourStatisticsCloumn.voltageHourColumns, VoltageHourTransfor.class).getData();
            if(flag)
                return new IntegrateHourVo(obdHourTransfor==null?new ObdHourTransfor():obdHourTransfor, gpsHourTransfor==null?new GpsHourTransfor():gpsHourTransfor, amHourTransfor==null?new AmHourTransfor():amHourTransfor,
                        deHourTransfor==null?new DeHourTransfor():deHourTransfor, traceHourTransfor==null?new TraceHourTransfor():traceHourTransfor,
                        traceDeleteHourTransfor==null?new TraceDeleteHourTransfor():traceDeleteHourTransfor,
                        voltageHourTransfor==null?new VoltageHourTransfor():voltageHourTransfor, strs,null);
            else
                return new IntegrateHourVo(obdHourTransfor==null?new ObdHourTransfor():obdHourTransfor, gpsHourTransfor==null?new GpsHourTransfor():gpsHourTransfor, amHourTransfor==null?new AmHourTransfor():amHourTransfor,
                        deHourTransfor==null?new DeHourTransfor():deHourTransfor, traceHourTransfor==null?new TraceHourTransfor():traceHourTransfor,
                        traceDeleteHourTransfor==null?new TraceDeleteHourTransfor():traceDeleteHourTransfor,
                        voltageHourTransfor==null?new VoltageHourTransfor():voltageHourTransfor, null,null);

        } catch (Throwable e) {
            e.printStackTrace();
        }
        return  new IntegrateHourVo();
    }

    public List<IntegrateHourVo> getHourDatasFromCarIds(boolean flag,long datetime,String[] carIds){
        List<IntegrateHourVo> list = new LinkedList<>();
        Arrays.asList(carIds).parallelStream().forEach(carId->{
            list.add(getHourFromCarId(flag,datetime,carId,null));
        });
        return list;
    }


    //day

    public List<IntegrateDayVo> getDayDatasFromFile(boolean flag, MultipartFile file, String originalName, String findDate) {

        File dest = new File(fileProperties.getOutPath(),originalName );
        try {
            file.transferTo(dest);
        } catch (IOException e) {
            log.error("exception:",e);
        }
        List<IntegrateDayVo> list = new LinkedList();
        try (Stream<String> stream = Files.lines(Paths.get(dest.toURI()))){
            stream.map(s->s.split(",")).forEach(strs->{
                long times;
                try {
                    times = DateTimeUtil.strToTimestamp(findDate, "yyyy-MM-dd");
                    log.info("str is {},and times is {}", findDate, times);
                    IntegrateDayVo dayvo = getDayFromCarId(flag, times, strs[0], strs);
                    if(dayvo!=null)
                        list.add(dayvo);
                } catch (ParseException e) {
                    e.printStackTrace();
                }

            });
        } catch (IOException e) {
            log.error("data error",e);
        }
        return list;
    }


    public List<IntegrateDayVo> getDayDatasFromFileWithOriginal(boolean flag, MultipartFile file, String originalName, String findDate) {

        File dest = new File(fileProperties.getOutPath(),originalName );
        try {
            file.transferTo(dest);
        } catch (IOException e) {
            log.error("exception:",e);
        }
        List<IntegrateDayVo> list = new LinkedList();
        try (Stream<String> stream = Files.lines(Paths.get(dest.toURI()))){
            stream.map(s->s.split(",")).forEach(strs->{
                long times;
                try {
                    times = DateTimeUtil.strToTimestamp(findDate, "yyyy-MM-dd");
                    log.info("str is {},and times is {}", findDate, times);
                    String dateTimeFrom = String.format(DAY_DATE_FROM, findDate);
                    String dateTimeTo = String.format(DAY_DATE_TO, findDate);

                    IntegrateDayVo dayvo = getDayFromCarId(flag, times, strs[0], strs);
                    if (dayvo != null) {
                        IntegrateDayVo nextVo = cstStramDataValidateIntegratedService.createDayIntegrated(strs[0], dateTimeFrom, dateTimeTo);
                        dayvo.setIntegrateDayVo(nextVo);
                        list.add(dayvo);
                    }
                } catch (ParseException e) {
                    e.printStackTrace();
                }

            });
        } catch (IOException e) {
            log.error("data error",e);
        }
        return list;
    }


    public  IntegrateDayVo getDayFromCarId(boolean flag,Long datetime,String carId,String[] strs){
        try {

            log.info("carId:{},times:{}",carId,datetime.longValue());

            DeDayTransfor deDayTransfor=(DeDayTransfor)cstStreamDayIntegrateService.getDayTransfor(carId,datetime.longValue() ,
                    HBaseTable.DAY_STATISTICS.getFourthFamilyName(), HbaseColumn.DayStatisticsCloumn.deDayColumns, DeDayTransfor.class).getData();
            log.info("deDayTransfor is :{}",deDayTransfor);
            ObdDayTransfor obdDayTransfor=(ObdDayTransfor)cstStreamDayIntegrateService.getDayTransfor(carId,datetime.longValue(),
                    HBaseTable.DAY_STATISTICS.getFirstFamilyName(), HbaseColumn.DayStatisticsCloumn.obdDayColumns, ObdDayTransfor.class).getData();
            log.info("obdDayTransfor is :{}",obdDayTransfor);
            GpsDayTransfor gpsDayTransfor=(GpsDayTransfor)cstStreamDayIntegrateService.getDayTransfor(carId, datetime.longValue(),
                    HBaseTable.DAY_STATISTICS.getSecondFamilyName(), HbaseColumn.DayStatisticsCloumn.gpsDayColumns, GpsDayTransfor.class).getData();
            log.info("gpsDayTransfor is :{}",gpsDayTransfor);
            AmDayTransfor amDayTransfor=(AmDayTransfor)cstStreamDayIntegrateService.getDayTransfor(carId, datetime.longValue(),
                    HBaseTable.DAY_STATISTICS.getThirdFamilyName(), HbaseColumn.DayStatisticsCloumn.amDayColumns, AmDayTransfor.class).getData();
            log.info("amDayTransfor is :{}",amDayTransfor);
            TraceDayTransfor traceDayTransfor=(TraceDayTransfor)cstStreamDayIntegrateService.getDayTransfor(carId, datetime.longValue(),
                    HBaseTable.DAY_STATISTICS.getFifthFamilyName(), HbaseColumn.DayStatisticsCloumn.traceDayColumns, TraceDayTransfor.class).getData();
            log.info("traceDayTransfor is :{}",traceDayTransfor);
            TraceDeleteDayTransfor traceDeleteDayTransfor=(TraceDeleteDayTransfor)cstStreamDayIntegrateService.getDayTransfor(carId,datetime.longValue(),
                    HBaseTable.DAY_STATISTICS.getSixthFamilyName(), HbaseColumn.DayStatisticsCloumn.traceDeleteDayColumns, TraceDeleteDayTransfor.class).getData();
            log.info("traceDeleteDayTransfor is :{}",traceDeleteDayTransfor);
            VoltageDayTransfor voltageDayTransfor=(VoltageDayTransfor)cstStreamDayIntegrateService.getDayTransfor(carId,datetime.longValue(),
                    HBaseTable.DAY_STATISTICS.getSeventhFamilyName(), HbaseColumn.DayStatisticsCloumn.voltageDayColumns, VoltageDayTransfor.class).getData();
            log.info("voltageDayTransfor is :{}",voltageDayTransfor);
            if (flag) {
               IntegrateDayVo integrateDayVo=new IntegrateDayVo(obdDayTransfor == null ? new ObdDayTransfor() : obdDayTransfor, gpsDayTransfor == null ? new GpsDayTransfor() : gpsDayTransfor, amDayTransfor == null ? new AmDayTransfor() : amDayTransfor,
                        deDayTransfor == null ? new DeDayTransfor() : deDayTransfor, traceDayTransfor == null ? new TraceDayTransfor() : traceDayTransfor,
                        traceDeleteDayTransfor == null ? new TraceDeleteDayTransfor() : traceDeleteDayTransfor,
                        voltageDayTransfor == null ? new VoltageDayTransfor() : voltageDayTransfor, strs, null);
                log.info("integratedData is :{}",integrateDayVo);
                return integrateDayVo;
            } else {
                return new IntegrateDayVo(obdDayTransfor == null ? new ObdDayTransfor() : obdDayTransfor, gpsDayTransfor == null ? new GpsDayTransfor() : gpsDayTransfor, amDayTransfor == null ? new AmDayTransfor() : amDayTransfor,
                        deDayTransfor == null ? new DeDayTransfor() : deDayTransfor, traceDayTransfor == null ? new TraceDayTransfor() : traceDayTransfor,
                        traceDeleteDayTransfor == null ? new TraceDeleteDayTransfor() : traceDeleteDayTransfor,
                        voltageDayTransfor == null ? new VoltageDayTransfor() : voltageDayTransfor, null, null);
            }

        } catch (Throwable e) {
            e.printStackTrace();
        }
        return  null;
    }

    public static String percentGet(Double oldData,Double newData ){
        if(oldData==null||newData==null||oldData<=0)
            return "0.00%";


        NumberFormat percent = NumberFormat.getPercentInstance();
        percent.setGroupingUsed(false);
        percent.setMaximumFractionDigits(2);
        BigDecimal a = new BigDecimal(oldData);
        BigDecimal b = new BigDecimal(newData);
        return percent.format( b.subtract(a).divide(a, 4, RoundingMode.HALF_UP).abs().floatValue());
    }
    public static String percentGet(Float oldData,Float newData ){
        if(oldData==null||newData==null||oldData<=0)
            return "0.00%";

        NumberFormat percent = NumberFormat.getPercentInstance();
        percent.setGroupingUsed(false);
        percent.setMaximumFractionDigits(2);
        BigDecimal a = new BigDecimal(oldData);
        BigDecimal b = new BigDecimal(newData);
        return percent.format( b.subtract(a).divide(a, 4, RoundingMode.HALF_UP).abs().floatValue());
    }
    public static String percentGet(Integer oldData,Integer newData ){
        if(oldData==null||newData==null||oldData<=0)
            return "0.00%";

        NumberFormat percent = NumberFormat.getPercentInstance();
        percent.setGroupingUsed(false);
        percent.setMaximumFractionDigits(2);
        BigDecimal a = new BigDecimal(oldData);
        BigDecimal b = new BigDecimal(newData);
        return percent.format( b.subtract(a).divide(a, 4, RoundingMode.HALF_UP).abs().floatValue());
    }
    public static String percentGet(Double oldData,Integer newData ){
        if(oldData==null||newData==null||oldData<=0)
            return "0.00%";

        NumberFormat percent = NumberFormat.getPercentInstance();
        percent.setGroupingUsed(false);
        percent.setMaximumFractionDigits(2);
        BigDecimal a = new BigDecimal(oldData);
        BigDecimal b = new BigDecimal(newData);
        return percent.format( b.subtract(a).divide(a, 4, RoundingMode.HALF_UP).abs().floatValue());
    }
}

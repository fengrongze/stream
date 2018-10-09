package com.cst.bigdata.controller.integrated.statistics;

import com.cst.bigdata.service.integrate.CstStreamHourIntegrateService;
import com.cst.bigdata.service.integrate.CstStreamHourNoDelayIntegrateService;
import com.cst.stream.base.BaseResult;
import com.cst.stream.common.HBaseTable;
import com.cst.stream.common.HbaseColumn;
import com.cst.stream.stathour.CstStreamBaseResult;
import com.cst.stream.stathour.integrated.HourIntegratedTransfor;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import javax.validation.constraints.NotNull;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.cst.stream.common.BusinessMathUtil.calcAvarageSpeed;
import static com.cst.stream.common.BusinessMathUtil.calcFuelPerHundred;

/**
 * @author Johnney.Chiu
 * create on 2018/7/2 18:07
 * @Description 小时统计接口
 * @title
 */
@RestController
@RequestMapping("/stream/integrated/statistics/hour")
@Api(description = "整合小时数据的查询",tags = "hour_data",consumes = MediaType.APPLICATION_JSON_VALUE)
@Slf4j
public class HourIntegratedStatisticsController {

    @Autowired
    private CstStreamHourIntegrateService cstStreamHourIntegrateService;

    @Autowired
    private CstStreamHourNoDelayIntegrateService cstStreamHourNoDelayIntegrateService;

    final static Map<String, String[]> familyMap = new HashMap<String, String[]>() {{
        put(HBaseTable.HOUR_STATISTICS.getFirstFamilyName(), HbaseColumn.HourStatisticsCloumn.obdHourColumns);
        put(HBaseTable.HOUR_STATISTICS.getSecondFamilyName(), HbaseColumn.HourStatisticsCloumn.gpsHourColumns);
        put(HBaseTable.HOUR_STATISTICS.getThirdFamilyName(), HbaseColumn.HourStatisticsCloumn.amHourColumns);
        put(HBaseTable.HOUR_STATISTICS.getFourthFamilyName(), HbaseColumn.HourStatisticsCloumn.deHourColumns);
        put(HBaseTable.HOUR_STATISTICS.getFifthFamilyName(), HbaseColumn.HourStatisticsCloumn.traceHourColumns);
        put(HBaseTable.HOUR_STATISTICS.getSixthFamilyName(), HbaseColumn.HourStatisticsCloumn.traceDeleteHourColumns);
        put(HBaseTable.HOUR_STATISTICS.getSeventhFamilyName(), HbaseColumn.HourStatisticsCloumn.voltageHourColumns);
        put(HBaseTable.DAY_STATISTICS.getEighthFamilyName(), HbaseColumn.HourStatisticsCloumn.mileageHourColumns);
    }};

    @GetMapping(value="/find/{carId}/{time}")
    @ResponseBody
    @ApiOperation(value="小时时间戳数据查询",httpMethod="GET", notes = "小时时间戳数据查询",consumes = MediaType.APPLICATION_JSON_VALUE)
    public CstStreamBaseResult<HourIntegratedTransfor> getHourDataByCarIdAndTimestamp(
            @ApiParam(value = "车id", required = true) @PathVariable("carId") @NotNull String carId,
            @ApiParam(value = "时间戳", required = true) @PathVariable("time") @NotNull Long time){
        log.debug("########################integrated hour data carid:{}  , time:{}",carId, time);

        CstStreamBaseResult<HourIntegratedTransfor> result= cstStreamHourIntegrateService.getHourTransfor(carId, time,familyMap,
                HbaseColumn.HourStatisticsCloumn.allColumns, HourIntegratedTransfor.class);

        if (result != null && result.getData() != null) {
            result.getData().setFuelPerHundred(calcFuelPerHundred(result.getData().getFuel(), result.getData().getMileage()));
            result.getData().setAverageSpeed(calcAvarageSpeed(result.getData().getMileage(),result.getData().getDuration()));

        }

        return result;
    }

    @GetMapping(value="/findByDateTime/{carId}/{dateTime}")
    @ResponseBody
    @ApiOperation(value="小时字符串数据查询",httpMethod="GET", notes = "小时字符串数据查询",consumes = MediaType.APPLICATION_JSON_VALUE)
    public CstStreamBaseResult<HourIntegratedTransfor> getHourDataByCarIdAndTimeStr(
            @ApiParam(value = "车id", required = true) @PathVariable("carId") @NotNull String carId,
            @ApiParam(value = "时间 yyyy-MM-dd HH", required = true) @PathVariable  @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd HH") Date dateTime){
        log.debug("########################integrated hour data carid:{}  , time:{}",carId, dateTime.getTime());
        CstStreamBaseResult<HourIntegratedTransfor> result=cstStreamHourIntegrateService.getHourTransfor(carId, dateTime.getTime(),familyMap,
                HbaseColumn.HourStatisticsCloumn.allColumns, HourIntegratedTransfor.class);
        if (result != null && result.getData() != null) {
            result.getData().setFuelPerHundred(calcFuelPerHundred(result.getData().getFuel(), result.getData().getMileage()));
            result.getData().setAverageSpeed(calcAvarageSpeed(result.getData().getMileage(),result.getData().getDuration()));
        }
        return result;

    }

    @GetMapping(value="/zone/find/{carId}/{fromTime}/{toTime}")
    @ResponseBody
    @ApiOperation(value="小时时间戳区间数据查询",httpMethod="GET", notes = "小时时间戳区间数据查询",consumes = MediaType.APPLICATION_JSON_VALUE)
    public BaseResult<List<HourIntegratedTransfor>> getHourDataByCarIdAndTimestamps(
            @ApiParam(value = "车id", required = true) @PathVariable("carId") @NotNull String carId,
            @ApiParam(value = "时间戳", required = true) @PathVariable("fromTime") @NotNull Long fromTime,
            @ApiParam(value = "时间戳", required = true) @PathVariable("toTime") @NotNull Long toTime){
        log.debug("########################integrated hour data carid:{}  , fromTime:{},toTime:{}",carId, fromTime,toTime);
        BaseResult<List<HourIntegratedTransfor>> result = cstStreamHourIntegrateService.getHourTransforBetween(carId, fromTime, toTime, familyMap,
                HbaseColumn.HourStatisticsCloumn.allColumns, HourIntegratedTransfor.class);
        if (result != null && CollectionUtils.isNotEmpty(result.getData())) {
            result.getData().stream().filter(d -> d != null)
                    .forEach(d->{
                        d.setFuelPerHundred(calcFuelPerHundred(d.getFuel(), d.getMileage()));
                        d.setAverageSpeed(calcAvarageSpeed(d.getMileage(),d.getDuration()));
                    });
        }
        return result;
    }

    @GetMapping(value="/zone/findTimestampAndSize/{carId}/{fromTime}/{size}")
    @ResponseBody
    @ApiOperation(value="小时时间戳和size区间数据查询",httpMethod="GET", notes = "小时时间戳和size区间数据查询",consumes = MediaType.APPLICATION_JSON_VALUE)
    public BaseResult<List<HourIntegratedTransfor>> getHourDataByCarIdAndTimestampAndSize(
            @ApiParam(value = "车id", required = true) @PathVariable("carId") @NotNull String carId,
            @ApiParam(value = "时间戳", required = true) @PathVariable("fromTime") @NotNull Long fromTime,
            @ApiParam(value = "往前往后时间区间数", required = true) @PathVariable("size") @NotNull int size){
        Instant instantFrom = Instant.ofEpochMilli(fromTime);
        ZoneId zone = ZoneId.systemDefault();
        LocalDateTime localDateTimeFrom=LocalDateTime.ofInstant(instantFrom, zone);
        long toTime;
        BaseResult<List<HourIntegratedTransfor>> result = null;
        if(size>=0){
            toTime=localDateTimeFrom.plusHours(size).toInstant(ZoneOffset.of("+8")).toEpochMilli();
            log.debug("########################integrated hour data carid:{}  , fromTime:{},toTime:{}",carId, fromTime,toTime);
            result= cstStreamHourIntegrateService.getHourTransforBetween(carId, fromTime,toTime,familyMap,
                    HbaseColumn.HourStatisticsCloumn.allColumns, HourIntegratedTransfor.class);
        }else {
            toTime=localDateTimeFrom.minusHours(Math.abs(size)).toInstant(ZoneOffset.of("+8")).toEpochMilli();
            log.debug("########################integrated hour data carid:{}  , fromTime:{},toTime:{}",carId, fromTime,toTime);
            result= cstStreamHourIntegrateService.getHourTransforBetween(carId, toTime,fromTime,familyMap,
                    HbaseColumn.HourStatisticsCloumn.allColumns, HourIntegratedTransfor.class);
        }
        if (result != null && CollectionUtils.isNotEmpty(result.getData())) {
            result.getData().stream().filter(d->d!=null)
                    .forEach(d->{
                        d.setFuelPerHundred(calcFuelPerHundred(d.getFuel(), d.getMileage()));
                        d.setAverageSpeed(calcAvarageSpeed(d.getMileage(),d.getDuration()));
                    });
        }

        return result;
    }

    @GetMapping(value="/zone/findByDateTime/{carId}/{fromTime}/{toTime}")
    @ResponseBody
    @ApiOperation(value="小时字符区间数据查询",httpMethod="GET", notes = "小时字符区间数据查询",consumes = MediaType.APPLICATION_JSON_VALUE)
    public BaseResult<List<HourIntegratedTransfor>> getHourDataByCarIdAndTimeStrs(
            @ApiParam(value = "车id", required = true) @PathVariable("carId") @NotNull String carId,
            @ApiParam(value = "时间 yyyy-MM-dd HH", required = true) @PathVariable("fromTime") @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd HH") Date fromTime,
            @ApiParam(value = "时间 yyyy-MM-dd HH", required = true) @PathVariable("toTime") @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd HH") Date toTime){
        log.debug("########################integrated hour data carid:{}  , fromTime:{} ,toTime:{}",carId, fromTime.getTime(),toTime.getTime());
        BaseResult<List<HourIntegratedTransfor>> result = cstStreamHourIntegrateService.getHourTransforBetween(carId, fromTime.getTime(), toTime.getTime(), familyMap,
                HbaseColumn.HourStatisticsCloumn.allColumns, HourIntegratedTransfor.class);
        if (result != null && CollectionUtils.isNotEmpty(result.getData())) {
            result.getData().stream().filter(d->d!=null)
                    .forEach(d->{
                        d.setFuelPerHundred(calcFuelPerHundred(d.getFuel(), d.getMileage()));
                        d.setAverageSpeed(calcAvarageSpeed(d.getMileage(),d.getDuration()));
                    });
        }
        return result;
    }




    @GetMapping(value="/nodelay/find/{carId}")
    @ResponseBody
    @ApiOperation(value="小时实时数据查询",httpMethod="GET", notes = "小时实时数据查询",consumes = MediaType.APPLICATION_JSON_VALUE)
    public CstStreamBaseResult<HourIntegratedTransfor> getHourDataNoDelayByCarIdAndTimestamp(
            @ApiParam(value = "车id", required = true) @PathVariable("carId") @NotNull String carId){
        log.debug("########################integrated hour nodelay data carid:{}",carId);
        return cstStreamHourNoDelayIntegrateService.getHourNoDelayDataHourTransfor(carId);
    }




}

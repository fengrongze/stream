package com.cst.bigdata.controller.integrated.statistics;

import com.cst.bigdata.service.integrate.CstStreamMonthIntegrateService;
import com.cst.stream.base.BaseResult;
import com.cst.stream.common.HBaseTable;
import com.cst.stream.common.HbaseColumn;
import com.cst.stream.stathour.CstStreamBaseResult;
import com.cst.stream.stathour.integrated.MonthIntegratedTransfor;
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
 * @Description 月统计接口
 * @title
 */

@RestController
@RequestMapping("/stream/integrated/statistics/month")
@Api(description = "整合月数据的查询", tags = "month_data",consumes = MediaType.APPLICATION_JSON_VALUE)
@Slf4j
public class MonthIntegratedStatisticsController {

    @Autowired
    private CstStreamMonthIntegrateService cstStreamMonthIntegrateService;


    final static Map<String, String[]> familyMap = new HashMap<String, String[]>() {{
        put(HBaseTable.MONTH_STATISTICS.getFirstFamilyName(), HbaseColumn.MonthStatisticsCloumn.obdMonthColumns);
        put(HBaseTable.MONTH_STATISTICS.getSecondFamilyName(), HbaseColumn.MonthStatisticsCloumn.gpsMonthColumns);
        put(HBaseTable.MONTH_STATISTICS.getThirdFamilyName(), HbaseColumn.MonthStatisticsCloumn.amMonthColumns);
        put(HBaseTable.MONTH_STATISTICS.getFourthFamilyName(), HbaseColumn.MonthStatisticsCloumn.deMonthColumns);
        put(HBaseTable.MONTH_STATISTICS.getFifthFamilyName(), HbaseColumn.MonthStatisticsCloumn.traceMonthColumns);
        put(HBaseTable.MONTH_STATISTICS.getSixthFamilyName(), HbaseColumn.MonthStatisticsCloumn.traceDeleteMonthColumns);
        put(HBaseTable.MONTH_STATISTICS.getSeventhFamilyName(), HbaseColumn.MonthStatisticsCloumn.voltageMonthColumns);
        put(HBaseTable.MONTH_STATISTICS.getEighthFamilyName(), HbaseColumn.MonthStatisticsCloumn.mileageMonthColumns);
    }};

    @GetMapping(value="/find/{carId}/{time}")
    @ResponseBody
    @ApiOperation(value="月时间戳数据查询",httpMethod="GET", notes = "月时间戳数据查询",consumes = MediaType.APPLICATION_JSON_VALUE)
    public CstStreamBaseResult<MonthIntegratedTransfor> getMonthDataByCarIdAndTimestamp(
            @ApiParam(value = "车id", required = true) @PathVariable("carId") @NotNull String carId,
            @ApiParam(value = "时间戳", required = true) @PathVariable("time") @NotNull Long time){
        log.debug("########################integrated month data carid:{}  , time:{}",carId, time);
        CstStreamBaseResult<MonthIntegratedTransfor> result = cstStreamMonthIntegrateService.getMonthTransfor(carId, time, familyMap,
                HbaseColumn.MonthStatisticsCloumn.allColumns, MonthIntegratedTransfor.class);
        if (result != null && result.getData() != null) {
            result.getData().setFuelPerHundred(calcFuelPerHundred(result.getData().getFuel(), result.getData().getMileage()));
            result.getData().setAverageSpeed(calcAvarageSpeed(result.getData().getMileage(),result.getData().getDuration()));

        }
        return result;
    }
    @GetMapping(value="/findByCarIds/{time}")
    @ResponseBody
    @ApiOperation(value="多个car id月时间戳数据查询",httpMethod="GET", notes = "多个car id月时间戳数据查询",consumes = MediaType.APPLICATION_JSON_VALUE)
    public BaseResult<Map<String,MonthIntegratedTransfor>> getMonthDataByCarIdsAndTimestamp(
            @ApiParam(value = "车ids", required = true) @RequestParam @NotNull String[]  carIds,
            @ApiParam(value = "时间戳", required = true) @PathVariable("time") @NotNull Long time){
        log.debug("########################integrated month data carids:{}  , time:{}",carIds, time);
        BaseResult<Map<String, MonthIntegratedTransfor>> result = cstStreamMonthIntegrateService.getMonthTransforCarIds(carIds, time, familyMap,
                HbaseColumn.MonthStatisticsCloumn.allColumns, MonthIntegratedTransfor.class);

        if (result != null && result.getData() != null) {
            result.getData().entrySet().stream().filter(entry -> entry.getValue() != null)
                    .forEach(entry->{
                        entry.getValue().setAverageSpeed(calcAvarageSpeed(entry.getValue().getMileage(),entry.getValue().getDuration()));
                        entry.getValue().setFuelPerHundred(calcFuelPerHundred(entry.getValue().getFuel(), entry.getValue().getMileage()));
                    });
        }

        return result;
    }

    @GetMapping(value="/findByDateTime/{carId}/{time}")
    @ResponseBody
    @ApiOperation(value="月字符串数据查询",httpMethod="GET", notes = "月字符串数据查询",consumes = MediaType.APPLICATION_JSON_VALUE)
    public CstStreamBaseResult<MonthIntegratedTransfor> getMonthDataByCarIdAndTimeStr(
            @ApiParam(value = "车id", required = true) @PathVariable("carId") @NotNull String carId,
            @ApiParam(value = "时间 yyyy-MM-dd", required = true) @PathVariable("time") @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd") Date dateTime){
        log.debug("########################integrated month data carid:{}  , time:{}",carId, dateTime.getTime());
        CstStreamBaseResult<MonthIntegratedTransfor> result = cstStreamMonthIntegrateService.getMonthTransfor(carId, dateTime.getTime(), familyMap,
                HbaseColumn.MonthStatisticsCloumn.allColumns, MonthIntegratedTransfor.class);
        if (result != null && result.getData() != null) {
            result.getData().setFuelPerHundred(calcFuelPerHundred(result.getData().getFuel(), result.getData().getMileage()));
            result.getData().setAverageSpeed(calcAvarageSpeed(result.getData().getMileage(),result.getData().getDuration()));
        }

        return result;

    }


    @GetMapping(value="/find/{carId}/{fromTime}/{toTime}")
    @ResponseBody
    @ApiOperation(value="月时间戳区间数据查询",httpMethod="GET", notes = "月时间戳区间数据查询",consumes = MediaType.APPLICATION_JSON_VALUE)
    public BaseResult<List<MonthIntegratedTransfor>> getMonthDataByCarIdAndTimestamps(
            @ApiParam(value = "车id", required = true) @PathVariable("carId") @NotNull String carId,
            @ApiParam(value = "时间戳", required = true) @PathVariable("fromTime") @NotNull Long fromTime,
            @ApiParam(value = "时间戳", required = true) @PathVariable("toTime") @NotNull Long toTime){
        log.debug("########################integrated month data carid:{}  , fromTime:{},toTime:{}",carId, fromTime,toTime);
        BaseResult<List<MonthIntegratedTransfor>> result = cstStreamMonthIntegrateService.getMonthTransforBetween(carId, fromTime, toTime, familyMap,
                HbaseColumn.MonthStatisticsCloumn.allColumns, MonthIntegratedTransfor.class);

        if (result != null && CollectionUtils.isNotEmpty(result.getData())) {
            result.getData().stream().filter(d -> d != null)
                    .forEach(d->{
                        d.setFuelPerHundred(calcFuelPerHundred(d.getFuel(), d.getMileage()));
                        d.setAverageSpeed(calcAvarageSpeed(d.getMileage(),d.getDuration()));
                    });
        }
        return result;
    }

    @GetMapping(value="/findTimestampAndSize/{carId}/{fromTime}/{size}")
    @ResponseBody
    @ApiOperation(value="月时间戳and size区间数据查询",httpMethod="GET", notes = "月时间戳and size区间数据查询",consumes = MediaType.APPLICATION_JSON_VALUE)
    public BaseResult<List<MonthIntegratedTransfor>> getMonthDataByCarIdAndTimestampAndSize(
            @ApiParam(value = "车id", required = true) @PathVariable("carId") @NotNull String carId,
            @ApiParam(value = "时间戳", required = true) @PathVariable("fromTime") @NotNull Long fromTime,
            @ApiParam(value = "往前往后时间区间数", required = true) @PathVariable("size") @NotNull int size){
        Instant instantFrom = Instant.ofEpochMilli(fromTime);
        ZoneId zone = ZoneId.systemDefault();
        LocalDateTime localDateTimeFrom=LocalDateTime.ofInstant(instantFrom, zone);
        long toTime;
        BaseResult<List<MonthIntegratedTransfor>> result;
        if(size>=0){
            toTime=localDateTimeFrom.plusMonths(size).toInstant(ZoneOffset.of("+8")).toEpochMilli();
            log.debug("########################integrated month data carid:{}  , fromTime:{},toTime:{}",carId, fromTime,toTime);
            result= cstStreamMonthIntegrateService.getMonthTransforBetween(carId, fromTime,toTime,familyMap,
                    HbaseColumn.MonthStatisticsCloumn.allColumns, MonthIntegratedTransfor.class);
        }else {
            toTime=localDateTimeFrom.minusMonths(Math.abs(size)).toInstant(ZoneOffset.of("+8")).toEpochMilli();
            log.debug("########################integrated month data carid:{}  , fromTime:{},toTime:{}",carId, fromTime,toTime);
            result= cstStreamMonthIntegrateService.getMonthTransforBetween(carId, toTime,fromTime,familyMap,
                    HbaseColumn.MonthStatisticsCloumn.allColumns, MonthIntegratedTransfor.class);
        }
        if (result != null && CollectionUtils.isNotEmpty(result.getData())) {
            result.getData().stream().filter(d -> d != null)
                    .forEach(d->{
                        d.setFuelPerHundred(calcFuelPerHundred(d.getFuel(), d.getMileage()));
                        d.setAverageSpeed(calcAvarageSpeed(d.getMileage(),d.getDuration()));
                    });
        }

        return result;
    }

    @GetMapping(value="/findByDateTime/{carId}/{fromTime}/{toTime}")
    @ResponseBody
    @ApiOperation(value="月字符区间数据查询",httpMethod="GET", notes = "月字符区间数据查询",consumes = MediaType.APPLICATION_JSON_VALUE)
    public BaseResult<List<MonthIntegratedTransfor>> getMonthDataByCarIdAndTimeStrs(
            @ApiParam(value = "车id", required = true) @PathVariable("carId") @NotNull String carId,
            @ApiParam(value = "时间 yyyy-MM-dd", required = true) @PathVariable("fromTime") @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd") Date fromTime,
            @ApiParam(value = "时间 yyyy-MM-dd", required = true) @PathVariable("toTime") @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd") Date toTime){
        log.debug("########################integrated month data carid:{}  , fromTime:{} ,toTime:{}",carId, fromTime.getTime(),toTime.getTime());
        BaseResult<List<MonthIntegratedTransfor>> result = cstStreamMonthIntegrateService.getMonthTransforBetween(carId, fromTime.getTime(), toTime.getTime(), familyMap,
                HbaseColumn.MonthStatisticsCloumn.allColumns, MonthIntegratedTransfor.class);
        if (result != null && CollectionUtils.isNotEmpty(result.getData())) {
            result.getData().stream().filter(d -> d != null)
                    .forEach(d->{
                        d.setFuelPerHundred(calcFuelPerHundred(d.getFuel(), d.getMileage()));
                        d.setAverageSpeed(calcAvarageSpeed(d.getMileage(),d.getDuration()));
                    });
        }
        return result;
    }


}

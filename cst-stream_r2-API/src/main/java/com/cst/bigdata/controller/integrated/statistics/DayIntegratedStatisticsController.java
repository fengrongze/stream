package com.cst.bigdata.controller.integrated.statistics;

import com.cst.bigdata.service.integrate.CstStreamDayIntegrateService;
import com.cst.bigdata.service.integrate.CstStreamDayNoDelayIntegrateService;
import com.cst.stream.base.BaseResult;
import com.cst.stream.common.DateTimeUtil;
import com.cst.stream.common.HBaseTable;
import com.cst.stream.common.HbaseColumn;
import com.cst.stream.stathour.CstStreamBaseResult;
import com.cst.stream.stathour.integrated.DayIntegratedTransfor;
import com.cst.stream.stathour.integrated.HourIntegratedTransfor;
import com.cst.stream.stathour.integrated.DayIntegratedTransfor;
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
import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.cst.stream.common.BusinessMathUtil.calcAvarageSpeed;
import static com.cst.stream.common.BusinessMathUtil.calcFuelPerHundred;

/**
 * @author Johnney.Chiu
 * create on 2018/7/2 18:07
 * @Description 天统计接口
 * @title
 */

@RestController
@RequestMapping("/stream/integrated/statistics/day")
@Api(description = "整合天数据的查询", tags = "day_data",consumes = MediaType.APPLICATION_JSON_VALUE)
@Slf4j
public class DayIntegratedStatisticsController {

    @Autowired
    private CstStreamDayIntegrateService cstStreamDayIntegrateService;

    @Autowired
    private CstStreamDayNoDelayIntegrateService cstStreamDayNoDelayIntegrateService;

    final static Map<String, String[]> familyMap = new HashMap<String, String[]>() {{
        put(HBaseTable.DAY_STATISTICS.getFirstFamilyName(), HbaseColumn.DayStatisticsCloumn.obdDayColumns);
        put(HBaseTable.DAY_STATISTICS.getSecondFamilyName(), HbaseColumn.DayStatisticsCloumn.gpsDayColumns);
        put(HBaseTable.DAY_STATISTICS.getThirdFamilyName(), HbaseColumn.DayStatisticsCloumn.amDayColumns);
        put(HBaseTable.DAY_STATISTICS.getFourthFamilyName(), HbaseColumn.DayStatisticsCloumn.deDayColumns);
        put(HBaseTable.DAY_STATISTICS.getFifthFamilyName(), HbaseColumn.DayStatisticsCloumn.traceDayColumns);
        put(HBaseTable.DAY_STATISTICS.getSixthFamilyName(), HbaseColumn.DayStatisticsCloumn.traceDeleteDayColumns);
        put(HBaseTable.DAY_STATISTICS.getSeventhFamilyName(), HbaseColumn.DayStatisticsCloumn.voltageDayColumns);
        put(HBaseTable.DAY_STATISTICS.getEighthFamilyName(), HbaseColumn.DayStatisticsCloumn.mileageDayColumns);
    }};

    @GetMapping(value="/find/{carId}/{time}")
    @ResponseBody
    @ApiOperation(value="天时间戳数据查询",httpMethod="GET", notes = "天时间戳数据查询",consumes = MediaType.APPLICATION_JSON_VALUE)
    public CstStreamBaseResult<DayIntegratedTransfor> getDayDataByCarIdAndTimestamp(
            @ApiParam(value = "车id", required = true) @PathVariable("carId") @NotNull String carId,
            @ApiParam(value = "时间戳", required = true) @PathVariable("time") @NotNull Long time){
        log.debug("########################integrated day data carid:{}  , time:{}",carId, time);
        CstStreamBaseResult<DayIntegratedTransfor> result= cstStreamDayIntegrateService.getDayTransfor(carId, time, familyMap,
                HbaseColumn.DayStatisticsCloumn.allColumns, DayIntegratedTransfor.class);

        if (result!=null&&result.getData() != null) {
            result.getData().setFuelPerHundred(calcFuelPerHundred(result.getData().getFuel(), result.getData().getMileage()));
            result.getData().setAverageSpeed(calcAvarageSpeed(result.getData().getMileage(),result.getData().getDuration()));
        }
        return result;
    }

    @GetMapping(value="/findByCarIds/{time}")
    @ResponseBody
    @ApiOperation(value="多个car id天时间戳数据查询",httpMethod="GET", notes = "多个car id天时间戳数据查询",consumes = MediaType.APPLICATION_JSON_VALUE)
    public BaseResult<Map<String,DayIntegratedTransfor>> getDayDataByCarIdsAndTimestamp(
            @ApiParam(value = "车ids", required = true) @RequestParam @NotNull String[]  carIds,
            @ApiParam(value = "时间戳", required = true) @PathVariable("time") @NotNull Long time){
        log.debug("########################integrated day data carids:{}  , time:{}",carIds, time);
        BaseResult<Map<String,DayIntegratedTransfor>> result=cstStreamDayIntegrateService.getDayTransforCarIds(carIds, time, familyMap,
                HbaseColumn.DayStatisticsCloumn.allColumns, DayIntegratedTransfor.class);
        if(result!=null&&result.getData()!=null){
            result.getData().entrySet().stream().filter(entry->entry.getValue()!=null).forEach(entry->{
                entry.getValue().setAverageSpeed(calcAvarageSpeed(entry.getValue().getMileage(),entry.getValue().getDuration()));
                entry.getValue().setFuelPerHundred(calcFuelPerHundred(entry.getValue().getFuel(), entry.getValue().getMileage()));
            });
        }
        return result;


    }
    
    @GetMapping(value="/findByDateTime/{carId}/{time}")
    @ResponseBody
    @ApiOperation(value="天字符串数据查询",httpMethod="GET", notes = "天字符串数据查询",consumes = MediaType.APPLICATION_JSON_VALUE)
    public CstStreamBaseResult<DayIntegratedTransfor> getDayDataByCarIdAndTimeStr(
            @ApiParam(value = "车id", required = true) @PathVariable("carId") @NotNull String carId,
            @ApiParam(value = "时间 yyyy-MM-dd", required = true) @PathVariable("time") @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd") Date dateTime){
        log.debug("########################integrated day data carid:{}  , time:{}",carId, dateTime.getTime());
        CstStreamBaseResult<DayIntegratedTransfor> result= cstStreamDayIntegrateService.getDayTransfor(carId, dateTime.getTime(),familyMap,
                HbaseColumn.DayStatisticsCloumn.allColumns, DayIntegratedTransfor.class);
        if (result!=null&&result.getData() != null) {
            result.getData().setFuelPerHundred(calcFuelPerHundred(result.getData().getFuel(), result.getData().getMileage()));
            result.getData().setAverageSpeed(calcAvarageSpeed(result.getData().getMileage(),result.getData().getDuration()));
        }
        return result;
    }


    @GetMapping(value="/find/{carId}/{fromTime}/{toTime}")
    @ResponseBody
    @ApiOperation(value="天时间戳区间数据查询",httpMethod="GET", notes = "天时间戳区间数据查询",consumes = MediaType.APPLICATION_JSON_VALUE)
    public BaseResult<List<DayIntegratedTransfor>> getDayDataByCarIdAndTimestamps(
            @ApiParam(value = "车id", required = true) @PathVariable("carId") @NotNull String carId,
            @ApiParam(value = "时间戳", required = true) @PathVariable("fromTime") @NotNull Long fromTime,
            @ApiParam(value = "时间戳", required = true) @PathVariable("toTime") @NotNull Long toTime){
        log.debug("########################integrated day data carid:{}  , fromTime:{},toTime:{}",carId, fromTime,toTime);
        BaseResult<List<DayIntegratedTransfor>> results=cstStreamDayIntegrateService.getDayTransforBetween(carId, fromTime,toTime,familyMap,
                HbaseColumn.DayStatisticsCloumn.allColumns, DayIntegratedTransfor.class);
        if(results!=null|| CollectionUtils.isNotEmpty(results.getData())) {
            results.getData().stream().filter(d -> d != null).forEach(d -> {
                try {
                    d.setTime(DateTimeUtil.getDayBase(d.getTime()));
                    d.setFuelPerHundred(calcFuelPerHundred(d.getFuel(), d.getMileage()));
                    d.setAverageSpeed(calcAvarageSpeed(d.getMileage(),d.getDuration()));
                } catch (ParseException e) {
                    log.error("change time error {}",d.getTime());
                }
            });
        }
        return results;
    }

    @GetMapping(value="/findByCarIds/{fromTime}/{toTime}")
    @ResponseBody
    @ApiOperation(value="多个carIds天时间戳区间数据查询",httpMethod="GET", notes = "多个carIds天时间戳区间数据查询",consumes = MediaType.APPLICATION_JSON_VALUE)
    public BaseResult<Map<String,List<DayIntegratedTransfor>>> getDayDataByCarIdAndTimestamps(
            @ApiParam(value = "车ids", required = true) @RequestParam @NotNull String[]  carIds,
            @ApiParam(value = "时间戳", required = true) @PathVariable("fromTime") @NotNull Long fromTime,
            @ApiParam(value = "时间戳", required = true) @PathVariable("toTime") @NotNull Long toTime){
        log.debug("########################integrated day data carid:{}  , fromTime:{},toTime:{}",carIds, fromTime,toTime);
        BaseResult<Map<String,List<DayIntegratedTransfor>>> result= cstStreamDayIntegrateService.getDayTransforBetweenByCarIds(carIds, fromTime, toTime, familyMap,
                HbaseColumn.DayStatisticsCloumn.allColumns, DayIntegratedTransfor.class);
        if(result!=null&&result.getData()!=null){
            result.getData().entrySet().stream().filter(entry->CollectionUtils.isNotEmpty(entry.getValue()))
                    .forEach(entry->{
                        entry.getValue().stream().forEach(d->{
                            d.setFuelPerHundred(calcFuelPerHundred(d.getFuel(), d.getMileage()));
                            d.setAverageSpeed(calcAvarageSpeed(d.getMileage(),d.getDuration()));
                        });
                    });
        }
        return result;

    }

    @GetMapping(value="/findByDateTime/{carId}/{fromTime}/{toTime}")
    @ResponseBody
    @ApiOperation(value="天字符区间数据查询",httpMethod="GET", notes = "天字符区间数据查询",consumes = MediaType.APPLICATION_JSON_VALUE)
    public BaseResult<List<DayIntegratedTransfor>> getDayDataByCarIdAndTimeStrs(
            @ApiParam(value = "车id", required = true) @PathVariable("carId") @NotNull String carId,
            @ApiParam(value = "时间 yyyy-MM-dd", required = true) @PathVariable("fromTime") @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd") Date fromTime,
            @ApiParam(value = "时间 yyyy-MM-dd", required = true) @PathVariable("toTime") @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd") Date toTime){
        log.debug("########################integrated day data carid:{}  , fromTime:{} ,toTime:{}",carId, fromTime.getTime(),toTime.getTime());
        BaseResult<List<DayIntegratedTransfor>> results= cstStreamDayIntegrateService.getDayTransforBetween(carId, fromTime.getTime(),toTime.getTime(),familyMap,
                HbaseColumn.DayStatisticsCloumn.allColumns, DayIntegratedTransfor.class);
        if(results!=null|| CollectionUtils.isNotEmpty(results.getData())) {
            results.getData().parallelStream().filter(d -> d != null).forEach(d -> {
                try {
                    long newTime = DateTimeUtil.getDayBase(d.getTime());
                    float fuelPerHundred = calcFuelPerHundred(d.getFuel(), d.getMileage());
                    float averageSpeed = calcAvarageSpeed(d.getMileage(), d.getDuration());
                    d.setTime(newTime);
                    d.setFuelPerHundred(fuelPerHundred);
                    d.setAverageSpeed(averageSpeed);
                } catch (ParseException e) {
                    log.error("change time error {}",d.getTime());
                }
            });
        }
        return results;
    }


    @GetMapping(value="/nodelay/find/{carId}")
    @ResponseBody
    @ApiOperation(value="天实时数据查询",httpMethod="GET", notes = "天实时数据查询",consumes = MediaType.APPLICATION_JSON_VALUE)
    public CstStreamBaseResult<DayIntegratedTransfor> getDayDataNoDelayByCarIdAndTimestamps(
            @ApiParam(value = "车id", required = true) @PathVariable("carId") @NotNull String carId){
        log.debug("########################integrated no delay day data carid:{}  ",carId);
        CstStreamBaseResult<DayIntegratedTransfor> result = cstStreamDayNoDelayIntegrateService.getDayNoDelayDataDayTransfor(carId);
        if (result != null && result.getData() != null) {
            try {
                result.getData().setTime(DateTimeUtil.getDayBase(result.getData().getTime()));
            } catch (ParseException e) {
                log.error("change no delay time error {}",result);
            }
        }
        return result;
    }
}

package com.cst.bigdata.controller.integrated.statistics;

import com.cst.bigdata.service.integrate.CstStreamYearIntegrateService;
import com.cst.stream.base.BaseResult;
import com.cst.stream.common.HBaseTable;
import com.cst.stream.common.HbaseColumn;
import com.cst.stream.stathour.CstStreamBaseResult;
import com.cst.stream.stathour.integrated.YearIntegratedTransfor;
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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.cst.stream.common.BusinessMathUtil.calcAvarageSpeed;
import static com.cst.stream.common.BusinessMathUtil.calcFuelPerHundred;

/**
 * @author Johnney.Chiu
 * create on 2018/7/2 18:07
 * @Description 年统计接口
 * @title
 */

@RestController
@RequestMapping("/stream/integrated/statistics/year")
@Api(description = "整合年数据的查询", tags = "year_data",consumes = MediaType.APPLICATION_JSON_VALUE)
@Slf4j
public class YearIntegratedStatisticsController {

    @Autowired
    private CstStreamYearIntegrateService cstStreamYearIntegrateService;


    final static Map<String, String[]> familyMap = new HashMap<String, String[]>() {{
        put(HBaseTable.YEAR_STATISTICS.getFirstFamilyName(), HbaseColumn.YearStatisticsCloumn.obdYearColumns);
        put(HBaseTable.YEAR_STATISTICS.getSecondFamilyName(), HbaseColumn.YearStatisticsCloumn.gpsYearColumns);
        put(HBaseTable.YEAR_STATISTICS.getThirdFamilyName(), HbaseColumn.YearStatisticsCloumn.amYearColumns);
        put(HBaseTable.YEAR_STATISTICS.getFourthFamilyName(), HbaseColumn.YearStatisticsCloumn.deYearColumns);
        put(HBaseTable.YEAR_STATISTICS.getFifthFamilyName(), HbaseColumn.YearStatisticsCloumn.traceYearColumns);
        put(HBaseTable.YEAR_STATISTICS.getSixthFamilyName(), HbaseColumn.YearStatisticsCloumn.traceDeleteYearColumns);
        put(HBaseTable.YEAR_STATISTICS.getSeventhFamilyName(), HbaseColumn.YearStatisticsCloumn.voltageYearColumns);
        put(HBaseTable.YEAR_STATISTICS.getEighthFamilyName(), HbaseColumn.YearStatisticsCloumn.mileageYearColumns);
    }};

    @GetMapping(value="/find/{carId}/{time}")
    @ResponseBody
    @ApiOperation(value="年时间戳数据查询",httpMethod="GET", notes = "年时间戳数据查询",consumes = MediaType.APPLICATION_JSON_VALUE)
    public CstStreamBaseResult<YearIntegratedTransfor> getYearDataByCarIdAndTimestamp(
            @ApiParam(value = "车id", required = true) @PathVariable("carId") @NotNull String carId,
            @ApiParam(value = "时间戳", required = true) @PathVariable("time") @NotNull Long time){
        log.debug("########################integrated year data carid:{}  , time:{}",carId, time);
        CstStreamBaseResult<YearIntegratedTransfor> result= cstStreamYearIntegrateService.getYearTransfor(carId, time, familyMap,
                HbaseColumn.YearStatisticsCloumn.allColumns, YearIntegratedTransfor.class);
        if (result != null && result.getData() != null) {
            result.getData().setFuelPerHundred(calcFuelPerHundred(result.getData().getFuel(), result.getData().getMileage()));
            result.getData().setAverageSpeed(calcAvarageSpeed(result.getData().getMileage(),result.getData().getDuration()));

        }
        return result;
    }

    @GetMapping(value="/findByCarIds/{time}")
    @ResponseBody
    @ApiOperation(value="多个car id 年时间戳数据查询",httpMethod="GET", notes = "多个car id 年时间戳数据查询",consumes = MediaType.APPLICATION_JSON_VALUE)
    public BaseResult<Map<String,YearIntegratedTransfor>> getYearDataByCarIdsAndTimestamp(
            @ApiParam(value = "车ids", required = true) @RequestParam @NotNull String[]  carIds,
            @ApiParam(value = "时间戳", required = true) @PathVariable("time") @NotNull Long time){
        log.debug("########################integrated year data carids:{}  , time:{}",carIds, time);
        BaseResult<Map<String, YearIntegratedTransfor>> result = cstStreamYearIntegrateService.getYearTransforCarIds(carIds, time, familyMap,
                HbaseColumn.YearStatisticsCloumn.allColumns, YearIntegratedTransfor.class);
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
    @ApiOperation(value="年字符串数据查询",httpMethod="GET", notes = "年字符串数据查询",consumes = MediaType.APPLICATION_JSON_VALUE)
    public CstStreamBaseResult<YearIntegratedTransfor> getYearDataByCarIdAndTimeStr(
            @ApiParam(value = "车id", required = true) @PathVariable("carId") @NotNull String carId,
            @ApiParam(value = "时间 yyyy-MM-dd", required = true) @PathVariable("time") @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd") Date dateTime){
        log.debug("########################integrated year data carid:{}  , time:{}",carId, dateTime.getTime());
        CstStreamBaseResult<YearIntegratedTransfor> result= cstStreamYearIntegrateService.getYearTransfor(carId, dateTime.getTime(),familyMap,
                HbaseColumn.YearStatisticsCloumn.allColumns, YearIntegratedTransfor.class);
        if (result != null && result.getData() != null) {
            result.getData().setFuelPerHundred(calcFuelPerHundred(result.getData().getFuel(), result.getData().getMileage()));
            result.getData().setAverageSpeed(calcAvarageSpeed(result.getData().getMileage(),result.getData().getDuration()));

        }
        return result;

    }


    @GetMapping(value="/find/{carId}/{fromTime}/{toTime}")
    @ResponseBody
    @ApiOperation(value="年时间戳区间数据查询",httpMethod="GET", notes = "年时间戳区间数据查询",consumes = MediaType.APPLICATION_JSON_VALUE)
    public BaseResult<List<YearIntegratedTransfor>> getYearDataByCarIdAndTimestamps(
            @ApiParam(value = "车id", required = true) @PathVariable("carId") @NotNull String carId,
            @ApiParam(value = "时间戳", required = true) @PathVariable("fromTime") @NotNull Long fromTime,
            @ApiParam(value = "时间戳", required = true) @PathVariable("toTime") @NotNull Long toTime){
        log.debug("########################integrated year data carid:{}  , fromTime:{},toTime:{}",carId, fromTime,toTime);
        BaseResult<List<YearIntegratedTransfor>> result=cstStreamYearIntegrateService.getYearTransforBetween(carId, fromTime,toTime,familyMap,
                HbaseColumn.YearStatisticsCloumn.allColumns, YearIntegratedTransfor.class);

        if (result != null&&CollectionUtils.isNotEmpty(result.getData())) {
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
    @ApiOperation(value="年字符区间数据查询",httpMethod="GET", notes = "年字符区间数据查询",consumes = MediaType.APPLICATION_JSON_VALUE)
    public BaseResult<List<YearIntegratedTransfor>> getYearDataByCarIdAndTimeStrs(
            @ApiParam(value = "车id", required = true) @PathVariable("carId") @NotNull String carId,
            @ApiParam(value = "时间 yyyy-MM-dd", required = true) @PathVariable("fromTime") @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd") Date fromTime,
            @ApiParam(value = "时间 yyyy-MM-dd", required = true) @PathVariable("toTime") @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd") Date toTime){
        log.debug("########################integrated year data carid:{}  , fromTime:{} ,toTime:{}",carId, fromTime.getTime(),toTime.getTime());
        BaseResult<List<YearIntegratedTransfor>> result= cstStreamYearIntegrateService.getYearTransforBetween(carId, fromTime.getTime(),toTime.getTime(),familyMap,
                HbaseColumn.YearStatisticsCloumn.allColumns, YearIntegratedTransfor.class);

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

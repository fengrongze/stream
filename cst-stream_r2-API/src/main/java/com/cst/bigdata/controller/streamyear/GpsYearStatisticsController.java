package com.cst.bigdata.controller.streamyear;

import com.cst.bigdata.service.integrate.CstStreamYearIntegrateService;
import com.cst.stream.common.HBaseTable;
import com.cst.stream.common.HbaseColumn;
import com.cst.stream.stathour.CstStreamBaseResult;
import com.cst.stream.stathour.gps.GpsYearTransfor;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;

import javax.validation.constraints.NotNull;
import java.util.Date;

/**
 * @author Johnney.Chiu
 * create on 2018/4/17 10:57
 * @Description gps年数据接口
 * @title
 */
@RestController
@RequestMapping("/stream/year/statistics/gps")
@Api(description = "gps年数据的查询以及存储")
@Slf4j
public class GpsYearStatisticsController {

    @Autowired
    private CstStreamYearIntegrateService cstStreamYearIntegrateService;

    //通过carId 时间戳获取 gps year data
    @GetMapping(value="/find/{carId}/{time}")
    @ResponseBody
    @ApiOperation(value="gps 年数据查询",httpMethod="GET")
    public CstStreamBaseResult<GpsYearTransfor> getCstStreamGpsYearTransforByTimestamp(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间戳", required = true) @PathVariable @NotNull Long time){
        log.debug("########################gps get year  data:{}  , {}",carId, time);
        return cstStreamYearIntegrateService.getYearTransfor(carId, time, HBaseTable.YEAR_STATISTICS.getSecondFamilyName(),
                HbaseColumn.YearStatisticsCloumn.gpsYearColumns, GpsYearTransfor.class);
    }
    @GetMapping(value="/findByDate/{carId}/{date}")
    @ResponseBody
    @ApiOperation(value="gps 年数据查询",httpMethod="GET")
    public CstStreamBaseResult<GpsYearTransfor> getCstStreamGpsYearTransforByDate(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间 yyyy-MM-dd", required = true) @PathVariable @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd") Date year){
        log.debug("########################gps get year  data:{}  , {}",carId, year.getTime());
        return cstStreamYearIntegrateService.getYearTransfor(carId, year.getTime(),HBaseTable.YEAR_STATISTICS.getSecondFamilyName(),
                HbaseColumn.YearStatisticsCloumn.gpsYearColumns, GpsYearTransfor.class);
    }
    //entity put gps year data
    @PutMapping(value="/save")
    @ResponseBody
    @ApiOperation(value="gps 年数据存储",httpMethod="PUT")
    public CstStreamBaseResult<GpsYearTransfor> putCstStreamGpsYearTransfor(
            @ApiParam(value = "gps年数据结果", required = true) @RequestBody @NotNull GpsYearTransfor gpsYearTransfor){
        log.debug("########################gps saving year data:{}  , {}", gpsYearTransfor.getCarId(), gpsYearTransfor.getTime());
        return cstStreamYearIntegrateService.putYearTransfor(gpsYearTransfor,HBaseTable.YEAR_STATISTICS.getSecondFamilyName());
    }



}

package com.cst.bigdata.controller.streamday;

import com.cst.bigdata.service.integrate.CstStreamDayIntegrateService;
import com.cst.bigdata.service.integrate.CstStreamDayNoDelayIntegrateService;
import com.cst.stream.common.HBaseTable;
import com.cst.stream.common.HbaseColumn;
import com.cst.stream.common.StreamRedisConstants;
import com.cst.stream.common.StreamTypeDefine;
import com.cst.stream.stathour.CstStreamBaseResult;
import com.cst.stream.stathour.gps.GpsDayTransfor;
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
 * @Description gps天数据接口
 * @title
 */
@RestController
@RequestMapping("/stream/day/statistics/gps")
@Api(description = "gps天数据的查询以及存储")
@Slf4j
public class GpsDayStatisticsController {

    @Autowired
    private CstStreamDayIntegrateService cstStreamDayIntegrateService;

    @Autowired
    private CstStreamDayNoDelayIntegrateService cstStreamDayNoDelayIntegrateService;
    //通过carId 时间戳获取 gps day data
    @GetMapping(value="/find/{carId}/{time}")
    @ResponseBody
    @ApiOperation(value="gps 天数据查询",httpMethod="GET")
    public CstStreamBaseResult<GpsDayTransfor> getCstStreamGpsDayTransforByTimestamp(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间戳", required = true) @PathVariable @NotNull Long time){
        log.debug("########################gps get day  data:{}  , {}",carId, time);
        return cstStreamDayIntegrateService.getDayTransfor(carId, time, HBaseTable.DAY_STATISTICS.getSecondFamilyName(),
                HbaseColumn.DayStatisticsCloumn.gpsDayColumns, GpsDayTransfor.class);
    }
    @GetMapping(value="/findByDate/{carId}/{date}")
    @ResponseBody
    @ApiOperation(value="gps 天数据查询",httpMethod="GET")
    public CstStreamBaseResult<GpsDayTransfor> getCstStreamGpsDayTransforByDate(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间 yyyy-MM-dd", required = true) @PathVariable @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd") Date date){
        log.debug("########################gps get day  data:{}  , {}",carId, date.getTime());
        return cstStreamDayIntegrateService.getDayTransfor(carId, date.getTime(),HBaseTable.DAY_STATISTICS.getSecondFamilyName(),
                HbaseColumn.DayStatisticsCloumn.gpsDayColumns, GpsDayTransfor.class);
    }
    //entity put gps day data
    @PutMapping(value="/save")
    @ResponseBody
    @ApiOperation(value="gps 天数据存储",httpMethod="PUT")
    public CstStreamBaseResult<GpsDayTransfor> putCstStreamGpsDayTransfor(
            @ApiParam(value = "gps天数据结果", required = true) @RequestBody @NotNull GpsDayTransfor gpsDayTransfor){
        log.debug("########################gps saving day data:{}  , {}", gpsDayTransfor.getCarId(), gpsDayTransfor.getTime());
        return cstStreamDayIntegrateService.putDayTransfor(gpsDayTransfor,HBaseTable.DAY_STATISTICS.getSecondFamilyName());
    }

    //通过carId 时间戳获取 gps day data
    @GetMapping(value="/nodelay/find/{carId}")
    @ResponseBody
    @ApiOperation(value="gps nodelay  天数据查询",httpMethod="GET")
    public CstStreamBaseResult<GpsDayTransfor> getCstStreamGpsDayTransforByCarId(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId){
        log.debug("########################gps get day nodelay  data:{} ",carId);
        return cstStreamDayNoDelayIntegrateService
                .getGpsNoDelayDayTransfor(carId, StreamRedisConstants.DayKey.DAY_GPS);
    }


}

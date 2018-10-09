package com.cst.bigdata.controller.daysource;

import com.cst.bigdata.service.integrate.CstStreamDayFirstIntegrateService;
import com.cst.stream.common.HbaseColumn;
import com.cst.stream.common.StreamTypeDefine;
import com.cst.stream.stathour.CstStreamBaseResult;
import com.cst.stream.stathour.gps.GpsHourSource;
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
@RequestMapping("/stream/day/source/gps")
@Api(description = "gps第一条数据的查询和存储")
@Slf4j
public class GpsDaySourceStatisticsController {
    @Autowired
    private CstStreamDayFirstIntegrateService<GpsHourSource> cstStreamDayFirstIntegrateService;

    //通过carId 时间戳获取 gps day data
    @GetMapping(value="/find/{carId}/{time}")
    @ResponseBody
    @ApiOperation(value="gps 第一条数据查询",httpMethod="GET")
    public CstStreamBaseResult<GpsHourSource> getCstStreamGpsHourSourceByTimestamp(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间戳", required = true)@PathVariable @NotNull Long time){
        log.debug("########################gps get day source data:{}  , {}",carId, time);
        return cstStreamDayFirstIntegrateService.getDaySource(carId, time, StreamTypeDefine.GPS_TYPE,
                HbaseColumn.DaySourceColumn.gpsDayColumns, GpsHourSource.class);
    }
    @GetMapping(value="/findByDateTime/{carId}/{dateTime}")
    @ResponseBody
    @ApiOperation(value="gps 第一条数据查询",httpMethod="GET")
    public CstStreamBaseResult<GpsHourSource> getCstStreamGpsHourSourceDateTime(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间 yyyy-MM-dd", required = true)@PathVariable @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd") Date dateTime){
        log.debug("########################gps get day source data:{}  , {}",carId, dateTime.getTime());
        return cstStreamDayFirstIntegrateService.getDaySource(carId, dateTime.getTime(), StreamTypeDefine.GPS_TYPE,
                HbaseColumn.HourSourceColumn.gpsHourColumns, GpsHourSource.class);
    }
    //entity put gps day data
    @PutMapping(value="/save")
    @ResponseBody
    @ApiOperation(value="gps 小时第一条数据存储",httpMethod="PUT")
    public CstStreamBaseResult<GpsHourSource> putCstStreamGpsHourSource(
            @ApiParam(value = "gps 小时第一条数据存储", required = true)@RequestBody @NotNull GpsHourSource gpsHourSource){
        log.debug("########################gps saving day source data:{}  , {}", gpsHourSource.getCarId(), gpsHourSource.getTime());
        return cstStreamDayFirstIntegrateService.putDaySource(gpsHourSource,StreamTypeDefine.GPS_TYPE);
    }


}

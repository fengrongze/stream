package com.cst.bigdata.controller.streamhour;

import com.cst.bigdata.service.integrate.CstStreamHourIntegrateService;
import com.cst.bigdata.service.integrate.CstStreamHourNoDelayIntegrateService;
import com.cst.stream.common.HBaseTable;
import com.cst.stream.common.HbaseColumn;
import com.cst.stream.common.StreamRedisConstants;
import com.cst.stream.common.StreamTypeDefine;
import com.cst.stream.stathour.CstStreamBaseResult;
import com.cst.stream.stathour.gps.GpsHourTransfor;
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
 * @Description gps小时数据接口
 * @title
 */
@RestController
@RequestMapping("/stream/hour/statistics/gps")
@Api(description = "gps小时数据的查询和存储")
@Slf4j
public class GpsHourStatisticsController {
    @Autowired
    private CstStreamHourIntegrateService cstStreamHourIntegrateService;

    @Autowired
    private CstStreamHourNoDelayIntegrateService cstStreamHourNoDelayIntegrateService;

    //通过carId 时间戳获取 gps hour data
    @GetMapping(value="/find/{carId}/{time}")
    @ResponseBody
    @ApiOperation(value="gps 小时数据查询",httpMethod="GET")
    public CstStreamBaseResult<GpsHourTransfor> getCstStreamGpsHourTransforByTimestamp(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间戳", required = true)@PathVariable @NotNull Long time){
        log.debug("########################gps get hour  data:{}  , {}",carId, time);
        return cstStreamHourIntegrateService.getHourTransfor(carId, time,HBaseTable.HOUR_STATISTICS.getSecondFamilyName(),
                HbaseColumn.HourStatisticsCloumn.gpsHourColumns, GpsHourTransfor.class);
    }
    @GetMapping(value="/findByDateTime/{carId}/{dateTime}")
    @ResponseBody
    @ApiOperation(value="gps 小时数据查询",httpMethod="GET")
    public CstStreamBaseResult<GpsHourTransfor> getCstStreamGpsHourTransforDateTime(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间 yyyy-MM-dd HH", required = true)@PathVariable @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd HH") Date dateTime){
        log.debug("########################gps get hour  data:{}  , {}",carId, dateTime.getTime());
        return cstStreamHourIntegrateService.getHourTransfor(carId, dateTime.getTime(),HBaseTable.HOUR_STATISTICS.getSecondFamilyName(),
                HbaseColumn.HourStatisticsCloumn.gpsHourColumns, GpsHourTransfor.class);
    }
    //entity put gps hour data
    @PutMapping(value="/save")
    @ResponseBody
    @ApiOperation(value="gps 小时数据存储",httpMethod="PUT")
    public CstStreamBaseResult<GpsHourTransfor> putCstStreamGpsHourTransfor(
            @ApiParam(value = "gps 小时计算结果数据", required = true)@RequestBody @NotNull GpsHourTransfor gpsHourTransfor){
        log.debug("########################gps saving hour data:{}  , {}", gpsHourTransfor.getCarId(), gpsHourTransfor.getTime());
        return cstStreamHourIntegrateService.putHourTransfor(gpsHourTransfor, HBaseTable.HOUR_STATISTICS.getSecondFamilyName());
    }

    //通过carId 时间戳获取 gps hour data
    @GetMapping(value="/nodelay/find/{carId}")
    @ResponseBody
    @ApiOperation(value="gps nodelay小时数据查询",httpMethod="GET")
    public CstStreamBaseResult<GpsHourTransfor> getCstStreamGpsNoDelayHourTransforByCarId(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId){
        log.debug("########################gps  get hour no delay data:{} ",carId);
        return cstStreamHourNoDelayIntegrateService.getGpsNoDelayHourTransfor(carId,
                StreamRedisConstants.HourKey.HOUR_GPS);
    }

}

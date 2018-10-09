package com.cst.bigdata.controller.streamhour;

import com.cst.bigdata.service.integrate.CstStreamHourIntegrateService;
import com.cst.bigdata.service.integrate.CstStreamHourNoDelayIntegrateService;
import com.cst.stream.common.HBaseTable;
import com.cst.stream.common.HbaseColumn;
import com.cst.stream.common.StreamRedisConstants;
import com.cst.stream.common.StreamTypeDefine;
import com.cst.stream.stathour.CstStreamBaseResult;
import com.cst.stream.stathour.trace.TraceHourTransfor;
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
 * @Description trace小时数据接口
 * @title
 */
@RestController
@RequestMapping("/stream/hour/statistics/trace")
@Api(description = "trace小时数据的查询和存储")
@Slf4j
public class TraceHourStatisticsController {
    @Autowired
    private CstStreamHourIntegrateService cstStreamHourIntegrateService;

    @Autowired
    private CstStreamHourNoDelayIntegrateService cstStreamHourNoDelayIntegrateService;

    //通过carId 时间戳获取 trace  hour data
    @GetMapping(value="/find/{carId}/{time}")
    @ResponseBody
    @ApiOperation(value="trace  小时数据查询",httpMethod="GET")
    public CstStreamBaseResult<TraceHourTransfor> getCstStreamTraceHourTransforByTimestamp(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间戳", required = true)@PathVariable @NotNull Long time){
        log.debug("########################trace  get hour  data:{}  , {}",carId, time);
        return cstStreamHourIntegrateService.getHourTransfor(carId, time,HBaseTable.HOUR_STATISTICS.getFifthFamilyName(),
                HbaseColumn.HourStatisticsCloumn.traceHourColumns, TraceHourTransfor.class);
    }
    @GetMapping(value="/findByDateTime/{carId}/{dateTime}")
    @ResponseBody
    @ApiOperation(value="Trace 小时数据查询",httpMethod="GET")
    public CstStreamBaseResult<TraceHourTransfor> getCstStreamTraceHourTransforDateTime(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间 yyyy-MM-dd HH", required = true)@PathVariable @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd HH") Date dateTime){
        log.debug("########################Trace get hour  data:{}  , {}",carId, dateTime.getTime());
        return cstStreamHourIntegrateService.getHourTransfor(carId, dateTime.getTime(),HBaseTable.HOUR_STATISTICS.getFifthFamilyName(),
                HbaseColumn.HourStatisticsCloumn.traceHourColumns, TraceHourTransfor.class);
    }
    //entity put TraceDelete hour data
    @PutMapping(value="/save")
    @ResponseBody
    @ApiOperation(value="Trace 小时数据存储",httpMethod="PUT")
    public CstStreamBaseResult<TraceHourTransfor> putCstStreamTraceHourTransfor(
            @ApiParam(value = "Trace 小时计算结果数据", required = true)@RequestBody @NotNull TraceHourTransfor traceHourTransfor){
        log.debug("########################gps saving hour data:{}  , {}", traceHourTransfor.getCarId(), traceHourTransfor.getTime());
        return cstStreamHourIntegrateService.putHourTransfor(traceHourTransfor, HBaseTable.HOUR_STATISTICS.getFifthFamilyName());
    }

    //通过carId 时间戳获取 Trace hour data
    @GetMapping(value="/nodelay/find/{carId}")
    @ResponseBody
    @ApiOperation(value="Trace nodelay小时数据查询",httpMethod="GET")
    public CstStreamBaseResult<TraceHourTransfor> getCstStreamTraceNoDelayHourTransforByCarId(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId){
        log.debug("########################Trace  get hour no delay data:{} ",carId);
        return cstStreamHourNoDelayIntegrateService.getTraceNoDelayHourTransfor(carId, StreamRedisConstants.HourKey.HOUR_TRACE);
    }

}

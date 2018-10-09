package com.cst.bigdata.controller.streamhour;

import com.cst.bigdata.service.integrate.CstStreamHourIntegrateService;
import com.cst.bigdata.service.integrate.CstStreamHourNoDelayIntegrateService;
import com.cst.stream.common.HBaseTable;
import com.cst.stream.common.HbaseColumn;
import com.cst.stream.common.StreamRedisConstants;
import com.cst.stream.common.StreamTypeDefine;
import com.cst.stream.stathour.CstStreamBaseResult;
import com.cst.stream.stathour.tracedelete.TraceDeleteHourTransfor;
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
 * create on 2018/4/17 10:58
 * @Description 轨迹删除小时接口
 * @title
 */
@RestController
@RequestMapping("/stream/hour/statistics/tracedelete")
@Api(description = "tracedelete小时数据的查询和存储")
@Slf4j
public class TraceDeleteHourStatisticsController {
    @Autowired
    private CstStreamHourIntegrateService cstStreamHourIntegrateService;

    @Autowired
    private CstStreamHourNoDelayIntegrateService cstStreamHourNoDelayIntegrateService;

    //通过carId 时间戳获取 trace delete hour data
    @GetMapping(value="/find/{carId}/{time}")
    @ResponseBody
    @ApiOperation(value="trace delete 小时数据查询",httpMethod="GET")
    public CstStreamBaseResult<TraceDeleteHourTransfor> getCstStreamTraceDeleteHourTransforByTimestamp(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间戳", required = true)@PathVariable @NotNull Long time){
        log.debug("########################trace delete get hour  data:{}  , {}",carId, time);
        return cstStreamHourIntegrateService.getHourTransfor(carId, time, HBaseTable.HOUR_STATISTICS.getSixthFamilyName(),
                HbaseColumn.HourStatisticsCloumn.traceDeleteHourColumns, TraceDeleteHourTransfor.class);
    }
    @GetMapping(value="/findByDateTime/{carId}/{dateTime}")
    @ResponseBody
    @ApiOperation(value="TraceDelete 小时数据查询",httpMethod="GET")
    public CstStreamBaseResult<TraceDeleteHourTransfor> getCstStreamTraceDeleteHourTransforDateTime(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间 yyyy-MM-dd HH", required = true)@PathVariable @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd HH") Date dateTime){
        log.debug("########################Trace Delete get hour  data:{}  , {}",carId, dateTime.getTime());
        return cstStreamHourIntegrateService.getHourTransfor(carId, dateTime.getTime(),HBaseTable.HOUR_STATISTICS.getSixthFamilyName(),
                HbaseColumn.HourStatisticsCloumn.traceDeleteHourColumns, TraceDeleteHourTransfor.class);
    }
    //entity put TraceDelete hour data
    @PutMapping(value="/save")
    @ResponseBody
    @ApiOperation(value="TraceDelete 小时数据存储",httpMethod="PUT")
    public CstStreamBaseResult<TraceDeleteHourTransfor> putCstStreamTraceDeleteHourTransfor(
            @ApiParam(value = "Trace Delete 小时计算结果数据", required = true)@RequestBody @NotNull TraceDeleteHourTransfor traceDeleteHourTransfor){
        log.debug("########################gps saving hour data:{}  , {}", traceDeleteHourTransfor.getCarId(), traceDeleteHourTransfor.getTime());
        return cstStreamHourIntegrateService.putHourTransfor(traceDeleteHourTransfor,HBaseTable.HOUR_STATISTICS.getSixthFamilyName());
    }

    //通过carId 时间戳获取 TraceDelete hour data
    @GetMapping(value="/nodelay/find/{carId}")
    @ResponseBody
    @ApiOperation(value="TraceDelete nodelay小时数据查询",httpMethod="GET")
    public CstStreamBaseResult<TraceDeleteHourTransfor> getCstStreamTraceDeleteNoDelayHourTransforByCarId(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId){
        log.debug("########################TraceDelete  get hour no delay data:{} ",carId);
        return cstStreamHourNoDelayIntegrateService.getTraceDeleteNoDelayHourTransfor(carId,
                StreamRedisConstants.HourKey.HOUR_TRACE_DELETE);
    }

}

package com.cst.bigdata.controller.streamday;

import com.cst.bigdata.service.integrate.CstStreamDayIntegrateService;
import com.cst.bigdata.service.integrate.CstStreamDayNoDelayIntegrateService;
import com.cst.stream.common.HBaseTable;
import com.cst.stream.common.HbaseColumn;
import com.cst.stream.common.StreamRedisConstants;
import com.cst.stream.common.StreamTypeDefine;
import com.cst.stream.stathour.CstStreamBaseResult;
import com.cst.stream.stathour.tracedelete.TraceDeleteDayTransfor;
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
 * @Description 轨迹删除接口
 * @title
 */
@RestController
@RequestMapping("/stream/day/statistics/tracedelete")
@Api(description = "tracedelete天数据的查询以及存储")
@Slf4j
public class TraceDeleteDayStatisticsController {

    @Autowired
    private CstStreamDayIntegrateService cstStreamDayIntegrateService;

    @Autowired
    private CstStreamDayNoDelayIntegrateService cstStreamDayNoDelayIntegrateService;

    //通过carId 时间戳获取 Trace Delete day data
    @GetMapping(value="/find/{carId}/{time}")
    @ResponseBody
    @ApiOperation(value="Trace Delete 天数据查询",httpMethod="GET")
    public CstStreamBaseResult<TraceDeleteDayTransfor> getCstStreamTraceDeleteDayTransforByTimestamp(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间戳", required = true) @PathVariable @NotNull Long time){
        log.debug("########################Trace Delete get day data:{}  , {}",carId, time);
        return cstStreamDayIntegrateService.getDayTransfor(carId, time,HBaseTable.DAY_STATISTICS.getSixthFamilyName(),
                HbaseColumn.DayStatisticsCloumn.traceDeleteDayColumns, TraceDeleteDayTransfor.class);
    }
    @GetMapping(value="/findByDate/{carId}/{date}")
    @ResponseBody
    @ApiOperation(value="Trace Delete 天数据查询",httpMethod="GET")
    public CstStreamBaseResult<TraceDeleteDayTransfor> getCstStreamTraceDeleteDayTransforByDate(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间 yyyy-MM-dd", required = true) @PathVariable @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd") Date date){
        log.debug("########################trace get day data:{}  , {}",carId, date.getTime());
        return cstStreamDayIntegrateService.getDayTransfor(carId, date.getTime(),HBaseTable.DAY_STATISTICS.getSixthFamilyName(),
                HbaseColumn.DayStatisticsCloumn.traceDeleteDayColumns, TraceDeleteDayTransfor.class);
    }
    //entity put Trace Delete day data
    @PutMapping(value="/save")
    @ResponseBody
    @ApiOperation(value="Trace Delete 天数据存储",httpMethod="PUT")
    public CstStreamBaseResult<TraceDeleteDayTransfor> putCstStreamTraceDeleteDayTransfor(
            @ApiParam(value = "Trace Delete天数据结果", required = true) @RequestBody @NotNull TraceDeleteDayTransfor traceDeleteDayTransfor){
        log.debug("##############################trace saving day data:{}  , {}", traceDeleteDayTransfor.getCarId(), traceDeleteDayTransfor.getTime());
        return cstStreamDayIntegrateService.putDayTransfor(traceDeleteDayTransfor, HBaseTable.DAY_STATISTICS.getSixthFamilyName());
    }


    //通过carId 时间戳获取 Trace Delete day data
    @GetMapping(value="/nodelay/find/{carId}")
    @ResponseBody
    @ApiOperation(value="Trace Delete nodelay天数据查询",httpMethod="GET")
    public CstStreamBaseResult<TraceDeleteDayTransfor> getCstStreamTraceDeleteDayTransforByTimestamp(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId){
        log.debug("########################Trace Delete get nodelay day data:{}  ",carId);
        return cstStreamDayNoDelayIntegrateService.getTraceDeleteNoDelayDayTransfor(carId, StreamRedisConstants.DayKey.DAY_TRACE_DELETE);
    }

}

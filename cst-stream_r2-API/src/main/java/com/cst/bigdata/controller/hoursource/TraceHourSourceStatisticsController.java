package com.cst.bigdata.controller.hoursource;

import com.cst.bigdata.service.integrate.CstStreamHourFirstIntegrateService;
import com.cst.stream.common.HbaseColumn;
import com.cst.stream.common.StreamTypeDefine;
import com.cst.stream.stathour.CstStreamBaseResult;
import com.cst.stream.stathour.trace.TraceHourSource;
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
@RequestMapping("/stream/hour/source/trace")
@Api(description = "trace第一条数据的查询和存储")
@Slf4j
public class TraceHourSourceStatisticsController {
    @Autowired
    private CstStreamHourFirstIntegrateService<TraceHourSource> cstStreamHourFirstIntegrateService;



    //通过carId 时间戳获取 trace  hour data
    @GetMapping(value="/find/{carId}/{time}")
    @ResponseBody
    @ApiOperation(value="trace  第一条数据查询",httpMethod="GET")
    public CstStreamBaseResult<TraceHourSource> getCstStreamTraceHourSourceByTimestamp(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间戳", required = true)@PathVariable @NotNull Long time){
        log.debug("########################trace  get hour source data:{}  , {}",carId, time);
        return cstStreamHourFirstIntegrateService.getHourSource(carId, time, StreamTypeDefine.TRACE_TYPE,
                HbaseColumn.HourSourceColumn.traceHourColumns, TraceHourSource.class);
    }
    @GetMapping(value="/findByDateTime/{carId}/{dateTime}")
    @ResponseBody
    @ApiOperation(value="Trace 第一条数据查询",httpMethod="GET")
    public CstStreamBaseResult<TraceHourSource> getCstStreamTraceHourSourceDateTime(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间 yyyy-MM-dd HH", required = true)@PathVariable @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd HH") Date dateTime){
        log.debug("########################Trace get hour source data:{}  , {}",carId, dateTime.getTime());
        return cstStreamHourFirstIntegrateService.getHourSource(carId, dateTime.getTime(), StreamTypeDefine.TRACE_TYPE,
                HbaseColumn.HourSourceColumn.traceHourColumns, TraceHourSource.class);
    }
    //entity put TraceDelete hour data
    @PutMapping(value="/save")
    @ResponseBody
    @ApiOperation(value="Trace 小时第一条数据存储",httpMethod="PUT")
    public CstStreamBaseResult<TraceHourSource> putCstStreamTraceHourSource(
            @ApiParam(value = "Trace 小时第一条数据存储", required = true)@RequestBody @NotNull TraceHourSource traceHourSource){
        log.debug("########################gps saving hour source data:{}  , {}", traceHourSource.getCarId(), traceHourSource.getTime());
        return cstStreamHourFirstIntegrateService.putHourSource(traceHourSource,StreamTypeDefine.TRACE_TYPE);
    }


}

package com.cst.bigdata.controller.hoursource;

import com.cst.bigdata.service.integrate.CstStreamHourFirstIntegrateService;
import com.cst.stream.common.HbaseColumn;
import com.cst.stream.common.StreamTypeDefine;
import com.cst.stream.stathour.CstStreamBaseResult;
import com.cst.stream.stathour.tracedelete.TraceDeleteHourSource;
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
@RequestMapping("/stream/source/hour/tracedelete")
@Api(description = "tracedelete第一条数据的查询和存储")
@Slf4j
public class TraceDeleteHourSourceStatisticsController {
    @Autowired
    private CstStreamHourFirstIntegrateService<TraceDeleteHourSource> cstStreamHourFirstIntegrateService;


    //通过carId 时间戳获取 trace delete hour data
    @GetMapping(value="/find/{carId}/{time}")
    @ResponseBody
    @ApiOperation(value="trace delete 第一条数据查询",httpMethod="GET")
    public CstStreamBaseResult<TraceDeleteHourSource> getCstStreamTraceDeleteHourSourceByTimestamp(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间戳", required = true)@PathVariable @NotNull Long time){
        log.debug("########################trace delete get hour source data:{}  , {}",carId, time);
        return cstStreamHourFirstIntegrateService.getHourSource(carId, time, StreamTypeDefine.TRACE_DELETE_TYPE,
                HbaseColumn.HourSourceColumn.traceDeleteHourColumns, TraceDeleteHourSource.class);
    }
    @GetMapping(value="/findByDateTime/{carId}/{dateTime}")
    @ResponseBody
    @ApiOperation(value="TraceDelete 第一条数据查询",httpMethod="GET")
    public CstStreamBaseResult<TraceDeleteHourSource> getCstStreamTraceDeleteHourSourceDateTime(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间 yyyy-MM-dd HH", required = true)@PathVariable @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd HH") Date dateTime){
        log.debug("########################Trace Delete get hour source data:{}  , {}",carId, dateTime.getTime());
        return cstStreamHourFirstIntegrateService.getHourSource(carId,  dateTime.getTime(), StreamTypeDefine.TRACE_DELETE_TYPE,
                HbaseColumn.HourSourceColumn.traceDeleteHourColumns, TraceDeleteHourSource.class);
    }
    //entity put TraceDelete hour data
    @PutMapping(value="/save")
    @ResponseBody
    @ApiOperation(value="TraceDelete 小时第一条数据存储",httpMethod="PUT")
    public CstStreamBaseResult<TraceDeleteHourSource> putCstStreamTraceDeleteHourSource(
            @ApiParam(value = "Trace Delete 小时第一条数据存储", required = true)@RequestBody @NotNull TraceDeleteHourSource traceDeleteHourSource){
        log.debug("########################gps saving hour source data:{}  , {}", traceDeleteHourSource.getCarId(), traceDeleteHourSource.getTime());
        return cstStreamHourFirstIntegrateService.putHourSource(traceDeleteHourSource,StreamTypeDefine.TRACE_DELETE_TYPE);
    }


}

package com.cst.bigdata.controller.streammonth;

import com.cst.bigdata.service.integrate.CstStreamMonthIntegrateService;
import com.cst.stream.common.HBaseTable;
import com.cst.stream.common.HbaseColumn;
import com.cst.stream.stathour.CstStreamBaseResult;
import com.cst.stream.stathour.trace.TraceMonthTransfor;
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
 * @Description trace月数据接口
 * @title
 */
@RestController
@RequestMapping("/stream/month/statistics/trace")
@Api(description = "trace月数据的查询以及存储")
@Slf4j
public class TraceMonthStatisticsController {

    @Autowired
    private CstStreamMonthIntegrateService cstStreamMonthIntegrateService;

    //通过carId 时间戳获取 Trace  month data
    @GetMapping(value="/find/{carId}/{time}")
    @ResponseBody
    @ApiOperation(value="Trace  月数据查询",httpMethod="GET")
    public CstStreamBaseResult<TraceMonthTransfor> getCstStreamTraceMonthTransforByTimestamp(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间戳", required = true) @PathVariable @NotNull Long time){
        log.debug("########################Trace  get month data:{}  , {}",carId, time);
        return cstStreamMonthIntegrateService.getMonthTransfor(carId, time,HBaseTable.MONTH_STATISTICS.getFifthFamilyName(),
                HbaseColumn.MonthStatisticsCloumn.traceMonthColumns, TraceMonthTransfor.class);
    }
    @GetMapping(value="/findByDate/{carId}/{month}")
    @ResponseBody
    @ApiOperation(value="Trace  月数据查询",httpMethod="GET")
    public CstStreamBaseResult<TraceMonthTransfor> getCstStreamTraceMonthTransforByDate(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间 yyyy-MM-dd", required = true) @PathVariable @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd") Date month){
        log.debug("########################Trace get month data:{}  , {}",carId, month.getTime());
        return cstStreamMonthIntegrateService.getMonthTransfor(carId, month.getTime(), HBaseTable.MONTH_STATISTICS.getFifthFamilyName(),
                HbaseColumn.MonthStatisticsCloumn.traceMonthColumns, TraceMonthTransfor.class);
    }
    //entity put Trace  month data
    @PutMapping(value="/save")
    @ResponseBody
    @ApiOperation(value="Trace  月数据存储",httpMethod="PUT")
    public CstStreamBaseResult<TraceMonthTransfor> putCstStreamTraceMonthTransfor(
            @ApiParam(value = "Trace  月数据结果", required = true) @RequestBody @NotNull TraceMonthTransfor traceMonthTransfor){
        log.debug("##############################obd saving month data:{}  , {}", traceMonthTransfor.getCarId(), traceMonthTransfor.getTime());
        return cstStreamMonthIntegrateService.putMonthTransfor(traceMonthTransfor,HBaseTable.MONTH_STATISTICS.getFifthFamilyName());
    }


}

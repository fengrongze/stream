package com.cst.bigdata.controller.streammonth;

import com.cst.bigdata.service.integrate.CstStreamMonthIntegrateService;
import com.cst.stream.common.HBaseTable;
import com.cst.stream.common.HbaseColumn;
import com.cst.stream.stathour.CstStreamBaseResult;
import com.cst.stream.stathour.tracedelete.TraceDeleteMonthTransfor;
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
@RequestMapping("/stream/month/statistics/tracedelete")
@Api(description = "tracedelete月数据的查询以及存储")
@Slf4j
public class TraceDeleteMonthStatisticsController {

    @Autowired
    private CstStreamMonthIntegrateService cstStreamMonthIntegrateService;

    //通过carId 时间戳获取 Trace Delete month data
    @GetMapping(value="/find/{carId}/{time}")
    @ResponseBody
    @ApiOperation(value="Trace Delete 月数据查询",httpMethod="GET")
    public CstStreamBaseResult<TraceDeleteMonthTransfor> getCstStreamTraceDeleteMonthTransforByTimestamp(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间戳", required = true) @PathVariable @NotNull Long time){
        log.debug("########################Trace Delete get month data:{}  , {}",carId, time);
        return cstStreamMonthIntegrateService.getMonthTransfor(carId, time,HBaseTable.MONTH_STATISTICS.getSixthFamilyName(),
                HbaseColumn.MonthStatisticsCloumn.traceDeleteMonthColumns, TraceDeleteMonthTransfor.class);
    }
    @GetMapping(value="/findByDate/{carId}/{month}")
    @ResponseBody
    @ApiOperation(value="Trace Delete 月数据查询",httpMethod="GET")
    public CstStreamBaseResult<TraceDeleteMonthTransfor> getCstStreamTraceDeleteMonthTransforByDate(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间 yyyy-MM-dd", required = true) @PathVariable @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd") Date month){
        log.debug("########################trace get month data:{}  , {}",carId, month.getTime());
        return cstStreamMonthIntegrateService.getMonthTransfor(carId, month.getTime(),HBaseTable.MONTH_STATISTICS.getSixthFamilyName(),
                HbaseColumn.MonthStatisticsCloumn.traceDeleteMonthColumns, TraceDeleteMonthTransfor.class);
    }
    //entity put Trace Delete month data
    @PutMapping(value="/save")
    @ResponseBody
    @ApiOperation(value="Trace Delete 月数据存储",httpMethod="PUT")
    public CstStreamBaseResult<TraceDeleteMonthTransfor> putCstStreamTraceDeleteMonthTransfor(
            @ApiParam(value = "Trace Delete月数据结果", required = true) @RequestBody @NotNull TraceDeleteMonthTransfor traceDeleteMonthTransfor){
        log.debug("##############################trace saving month data:{}  , {}", traceDeleteMonthTransfor.getCarId(), traceDeleteMonthTransfor.getTime());
        return cstStreamMonthIntegrateService.putMonthTransfor(traceDeleteMonthTransfor, HBaseTable.MONTH_STATISTICS.getSixthFamilyName());
    }


}

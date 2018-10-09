package com.cst.bigdata.controller.streamyear;

import com.cst.bigdata.service.integrate.CstStreamYearIntegrateService;
import com.cst.stream.common.HBaseTable;
import com.cst.stream.common.HbaseColumn;
import com.cst.stream.stathour.CstStreamBaseResult;
import com.cst.stream.stathour.trace.TraceYearTransfor;
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
 * @Description trace年数据接口
 * @title
 */
@RestController
@RequestMapping("/stream/year/statistics/trace")
@Api(description = "trace年数据的查询以及存储")
@Slf4j
public class TraceYearStatisticsController {

    @Autowired
    private CstStreamYearIntegrateService cstStreamYearIntegrateService;

    //通过carId 时间戳获取 Trace  year data
    @GetMapping(value="/find/{carId}/{time}")
    @ResponseBody
    @ApiOperation(value="Trace  年数据查询",httpMethod="GET")
    public CstStreamBaseResult<TraceYearTransfor> getCstStreamTraceYearTransforByTimestamp(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间戳", required = true) @PathVariable @NotNull Long time){
        log.debug("########################Trace  get year data:{}  , {}",carId, time);
        return cstStreamYearIntegrateService.getYearTransfor(carId, time,HBaseTable.YEAR_STATISTICS.getFifthFamilyName(),
                HbaseColumn.YearStatisticsCloumn.traceYearColumns, TraceYearTransfor.class);
    }
    @GetMapping(value="/findByDate/{carId}/{year}")
    @ResponseBody
    @ApiOperation(value="Trace  年数据查询",httpMethod="GET")
    public CstStreamBaseResult<TraceYearTransfor> getCstStreamTraceYearTransforByDate(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间 yyyy-MM-dd", required = true) @PathVariable @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd") Date year){
        log.debug("########################Trace get year data:{}  , {}",carId, year.getTime());
        return cstStreamYearIntegrateService.getYearTransfor(carId, year.getTime(), HBaseTable.YEAR_STATISTICS.getFifthFamilyName(),
                HbaseColumn.YearStatisticsCloumn.traceYearColumns, TraceYearTransfor.class);
    }
    //entity put Trace  year data
    @PutMapping(value="/save")
    @ResponseBody
    @ApiOperation(value="Trace  年数据存储",httpMethod="PUT")
    public CstStreamBaseResult<TraceYearTransfor> putCstStreamTraceYearTransfor(
            @ApiParam(value = "Trace  年数据结果", required = true) @RequestBody @NotNull TraceYearTransfor traceYearTransfor){
        log.debug("##############################obd saving year data:{}  , {}", traceYearTransfor.getCarId(), traceYearTransfor.getTime());
        return cstStreamYearIntegrateService.putYearTransfor(traceYearTransfor,HBaseTable.YEAR_STATISTICS.getFifthFamilyName());
    }


}

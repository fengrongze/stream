package com.cst.bigdata.controller.streamyear;

import com.cst.bigdata.service.integrate.CstStreamYearIntegrateService;
import com.cst.stream.common.HBaseTable;
import com.cst.stream.common.HbaseColumn;
import com.cst.stream.stathour.CstStreamBaseResult;
import com.cst.stream.stathour.tracedelete.TraceDeleteYearTransfor;
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
@RequestMapping("/stream/year/statistics/tracedelete")
@Api(description = "tracedelete年数据的查询以及存储")
@Slf4j
public class TraceDeleteYearStatisticsController {

    @Autowired
    private CstStreamYearIntegrateService cstStreamYearIntegrateService;

    //通过carId 时间戳获取 Trace Delete year data
    @GetMapping(value="/find/{carId}/{time}")
    @ResponseBody
    @ApiOperation(value="Trace Delete 年数据查询",httpMethod="GET")
    public CstStreamBaseResult<TraceDeleteYearTransfor> getCstStreamTraceDeleteYearTransforByTimestamp(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间戳", required = true) @PathVariable @NotNull Long time){
        log.debug("########################Trace Delete get year data:{}  , {}",carId, time);
        return cstStreamYearIntegrateService.getYearTransfor(carId, time,HBaseTable.YEAR_STATISTICS.getSixthFamilyName(),
                HbaseColumn.YearStatisticsCloumn.traceDeleteYearColumns, TraceDeleteYearTransfor.class);
    }
    @GetMapping(value="/findByDate/{carId}/{year}")
    @ResponseBody
    @ApiOperation(value="Trace Delete 年数据查询",httpMethod="GET")
    public CstStreamBaseResult<TraceDeleteYearTransfor> getCstStreamTraceDeleteYearTransforByDate(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间 yyyy-MM-dd", required = true) @PathVariable @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd") Date year){
        log.debug("########################trace get year data:{}  , {}",carId, year.getTime());
        return cstStreamYearIntegrateService.getYearTransfor(carId, year.getTime(),HBaseTable.YEAR_STATISTICS.getSixthFamilyName(),
                HbaseColumn.YearStatisticsCloumn.traceDeleteYearColumns, TraceDeleteYearTransfor.class);
    }
    //entity put Trace Delete year data
    @PutMapping(value="/save")
    @ResponseBody
    @ApiOperation(value="Trace Delete 年数据存储",httpMethod="PUT")
    public CstStreamBaseResult<TraceDeleteYearTransfor> putCstStreamTraceDeleteYearTransfor(
            @ApiParam(value = "Trace Delete年数据结果", required = true) @RequestBody @NotNull TraceDeleteYearTransfor traceDeleteYearTransfor){
        log.debug("##############################trace saving year data:{}  , {}", traceDeleteYearTransfor.getCarId(), traceDeleteYearTransfor.getTime());
        return cstStreamYearIntegrateService.putYearTransfor(traceDeleteYearTransfor, HBaseTable.YEAR_STATISTICS.getSixthFamilyName());
    }


}

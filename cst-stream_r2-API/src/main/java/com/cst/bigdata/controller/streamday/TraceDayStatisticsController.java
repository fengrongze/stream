package com.cst.bigdata.controller.streamday;

import com.cst.bigdata.service.integrate.CstStreamDayIntegrateService;
import com.cst.bigdata.service.integrate.CstStreamDayNoDelayIntegrateService;
import com.cst.stream.common.HBaseTable;
import com.cst.stream.common.HbaseColumn;
import com.cst.stream.common.StreamRedisConstants;
import com.cst.stream.common.StreamTypeDefine;
import com.cst.stream.stathour.CstStreamBaseResult;
import com.cst.stream.stathour.trace.TraceDayTransfor;
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
 * @Description trace天数据接口
 * @title
 */
@RestController
@RequestMapping("/stream/day/statistics/trace")
@Api(description = "trace天数据的查询以及存储")
@Slf4j
public class TraceDayStatisticsController {

    @Autowired
    private CstStreamDayIntegrateService cstStreamDayIntegrateService;

    @Autowired
    private CstStreamDayNoDelayIntegrateService cstStreamDayNoDelayIntegrateService;
    //通过carId 时间戳获取 Trace  day data
    @GetMapping(value="/find/{carId}/{time}")
    @ResponseBody
    @ApiOperation(value="Trace  天数据查询",httpMethod="GET")
    public CstStreamBaseResult<TraceDayTransfor> getCstStreamTraceDayTransforByTimestamp(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间戳", required = true) @PathVariable @NotNull Long time){
        log.debug("########################Trace  get day data:{}  , {}",carId, time);
        return cstStreamDayIntegrateService.getDayTransfor(carId, time,HBaseTable.DAY_STATISTICS.getFifthFamilyName(),
                HbaseColumn.DayStatisticsCloumn.traceDayColumns, TraceDayTransfor.class);
    }
    @GetMapping(value="/findByDate/{carId}/{date}")
    @ResponseBody
    @ApiOperation(value="Trace  天数据查询",httpMethod="GET")
    public CstStreamBaseResult<TraceDayTransfor> getCstStreamTraceDayTransforByDate(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间 yyyy-MM-dd", required = true) @PathVariable @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd") Date date){
        log.debug("########################Trace get day data:{}  , {}",carId, date.getTime());
        return cstStreamDayIntegrateService.getDayTransfor(carId, date.getTime(), HBaseTable.DAY_STATISTICS.getFifthFamilyName(),
                HbaseColumn.DayStatisticsCloumn.traceDayColumns, TraceDayTransfor.class);
    }
    //entity put Trace  day data
    @PutMapping(value="/save")
    @ResponseBody
    @ApiOperation(value="Trace  天数据存储",httpMethod="PUT")
    public CstStreamBaseResult<TraceDayTransfor> putCstStreamTraceDayTransfor(
            @ApiParam(value = "Trace  天数据结果", required = true) @RequestBody @NotNull TraceDayTransfor traceDayTransfor){
        log.debug("##############################obd saving day data:{}  , {}", traceDayTransfor.getCarId(), traceDayTransfor.getTime());
        return cstStreamDayIntegrateService.putDayTransfor(traceDayTransfor,HBaseTable.DAY_STATISTICS.getFifthFamilyName());
    }


    //通过carId 时间戳获取 Trace  day data
    @GetMapping(value="/nodelay/find/{carId}")
    @ResponseBody
    @ApiOperation(value="Trace  nodelay天数据查询",httpMethod="GET")
    public CstStreamBaseResult<TraceDayTransfor> getCstStreamTraceDayTransforByTimestamp(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId){
        log.debug("########################Trace  get nodelay day data:{}  ",carId);
        return cstStreamDayNoDelayIntegrateService.getTraceNoDelayDayTransfor(carId, StreamRedisConstants.DayKey.DAY_TRACE);
    }

}

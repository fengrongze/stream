package com.cst.bigdata.controller.streamday;

import com.cst.bigdata.service.integrate.CstStreamDayIntegrateService;
import com.cst.bigdata.service.integrate.CstStreamDayNoDelayIntegrateService;
import com.cst.stream.common.HBaseTable;
import com.cst.stream.common.HbaseColumn;
import com.cst.stream.common.StreamRedisConstants;
import com.cst.stream.common.StreamTypeDefine;
import com.cst.stream.stathour.CstStreamBaseResult;
import com.cst.stream.stathour.am.AmDayTransfor;
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
 * create on 2018/4/17 10:55
 * @Description am天数据接口
 * @title
 */
@RestController
@RequestMapping("/stream/day/statistics/am")
@Api(description = "am天数据的查询以及存储")
@Slf4j
public class AmDayStatisticsController {

    @Autowired
    private CstStreamDayIntegrateService cstStreamDayIntegrateService;

    @Autowired
    private CstStreamDayNoDelayIntegrateService cstStreamDayNoDelayIntegrateService;

    @GetMapping(value="/find/{carId}/{time}")
    @ResponseBody
    @ApiOperation(value="am 天数据查询",httpMethod="GET")
    public CstStreamBaseResult<AmDayTransfor> getCstStreamAmDayTransforByTimestamp(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间戳", required = true) @PathVariable @NotNull Long time){
        log.debug("########################am get day data:{}  , {}",carId, time);
        return cstStreamDayIntegrateService.getDayTransfor(carId, time,HBaseTable.DAY_STATISTICS.getThirdFamilyName(),
                HbaseColumn.DayStatisticsCloumn.amDayColumns, AmDayTransfor.class);
    }
    @GetMapping(value="/findByDate/{carId}/{date}")
    @ResponseBody
    @ApiOperation(value="am 天数据查询",httpMethod="GET")
    public CstStreamBaseResult<AmDayTransfor> getCstStreamAmDayTransforByDate(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间 yyyy-MM-dd", required = true) @PathVariable @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd") Date date){
        log.debug("########################am get day data:{}  , {}",carId, date.getTime());
        return cstStreamDayIntegrateService.getDayTransfor(carId,  date.getTime(), HBaseTable.DAY_STATISTICS.getThirdFamilyName(),
                HbaseColumn.DayStatisticsCloumn.amDayColumns, AmDayTransfor.class);
    }
    @PutMapping(value="/save")
    @ResponseBody
    @ApiOperation(value="am 天数据存储",httpMethod="PUT")
    public CstStreamBaseResult<AmDayTransfor> putCstStreamAmDayTransfor(
            @ApiParam(value = "am天数据结果", required = true)  @RequestBody @NotNull AmDayTransfor amDayTransfor){
        log.debug("########################am saving data data:{}  , {}", amDayTransfor.getCarId(), amDayTransfor.getTime());
        return cstStreamDayIntegrateService.putDayTransfor(amDayTransfor,HBaseTable.DAY_STATISTICS.getThirdFamilyName());
    }
    @GetMapping(value="/nodelay/find/{carId}")
    @ResponseBody
    @ApiOperation(value="am nodelay 天数据查询",httpMethod="GET")
    public CstStreamBaseResult<AmDayTransfor> getCstStreamAmNoDelayDayTransforByCarId(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId){
        log.debug("########################am get nodelay day data:{} ",carId);
        return cstStreamDayNoDelayIntegrateService.getAmNoDelayDayTransfor(carId, StreamRedisConstants.DayKey.DAY_AM);
    }

}

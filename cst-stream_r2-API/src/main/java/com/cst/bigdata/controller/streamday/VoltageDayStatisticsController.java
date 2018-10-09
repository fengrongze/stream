package com.cst.bigdata.controller.streamday;

import com.cst.bigdata.service.integrate.CstStreamDayIntegrateService;
import com.cst.bigdata.service.integrate.CstStreamDayNoDelayIntegrateService;
import com.cst.stream.common.HBaseTable;
import com.cst.stream.common.HbaseColumn;
import com.cst.stream.common.StreamRedisConstants;
import com.cst.stream.common.StreamTypeDefine;
import com.cst.stream.stathour.CstStreamBaseResult;
import com.cst.stream.stathour.voltage.VoltageDayTransfor;
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
 * create on 2018/4/17 10:59
 * @Description 电瓶电压天数据接口
 * @title
 */
@RestController
@RequestMapping("/stream/day/statistics/voltage")
@Api(description = "voltage天数据的查询以及存储")
@Slf4j
public class VoltageDayStatisticsController {

    @Autowired
    private CstStreamDayIntegrateService cstStreamDayIntegrateService;

    @Autowired
    private CstStreamDayNoDelayIntegrateService cstStreamDayNoDelayIntegrateService;

    //通过carId 时间戳获取 Voltage  day data
    @GetMapping(value="/find/{carId}/{time}")
    @ResponseBody
    @ApiOperation(value="Voltage  天数据查询",httpMethod="GET")
    public CstStreamBaseResult<VoltageDayTransfor> getCstStreamVoltageDayTransforByTimestamp(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间戳", required = true) @PathVariable @NotNull Long time){
        log.debug("########################Voltage  get day data:{}  , {}",carId, time);
        return cstStreamDayIntegrateService.getDayTransfor(carId, time,HBaseTable.DAY_STATISTICS.getSeventhFamilyName(),
                HbaseColumn.DayStatisticsCloumn.voltageDayColumns, VoltageDayTransfor.class);
    }
    @GetMapping(value="/findByDate/{carId}/{date}")
    @ResponseBody
    @ApiOperation(value="Voltage  天数据查询",httpMethod="GET")
    public CstStreamBaseResult<VoltageDayTransfor> getCstStreamVoltageDayTransforByDate(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间 yyyy-MM-dd", required = true) @PathVariable @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd") Date date){
        log.debug("########################Voltage get day data:{}  , {}",carId, date.getTime());
        return cstStreamDayIntegrateService.getDayTransfor(carId, date.getTime(),HBaseTable.DAY_STATISTICS.getSeventhFamilyName(),
                HbaseColumn.DayStatisticsCloumn.voltageDayColumns, VoltageDayTransfor.class);
    }
    //entity put Voltage  day data
    @PutMapping(value="/save")
    @ResponseBody
    @ApiOperation(value="Voltage  天数据存储",httpMethod="PUT")
    public CstStreamBaseResult<VoltageDayTransfor> putCstStreamVoltageDayTransfor(
            @ApiParam(value = "Voltage  天数据结果", required = true) @RequestBody @NotNull VoltageDayTransfor voltageDayTransfor){
        log.debug("##############################obd saving day data:{}  , {}", voltageDayTransfor.getCarId(), voltageDayTransfor.getTime());
        return cstStreamDayIntegrateService.putDayTransfor(voltageDayTransfor, HBaseTable.DAY_STATISTICS.getSeventhFamilyName());
    }


    //通过carId 时间戳获取 Voltage  day data
    @GetMapping(value="/nodelay/find/{carId}")
    @ResponseBody
    @ApiOperation(value="Voltage  nodelay天数据查询",httpMethod="GET")
    public CstStreamBaseResult<VoltageDayTransfor> getCstStreamVoltageDayTransforByTimestamp(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId){
        log.debug("########################Voltage  get nodelay day data:{}  ",carId);
        return cstStreamDayNoDelayIntegrateService
                .getVoltageNoDelayDayTransfor(carId, StreamRedisConstants.DayKey.DAY_VOLTAGE);
    }

}

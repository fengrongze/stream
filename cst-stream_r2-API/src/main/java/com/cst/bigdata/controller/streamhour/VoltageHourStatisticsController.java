package com.cst.bigdata.controller.streamhour;

import com.cst.bigdata.service.integrate.CstStreamHourIntegrateService;
import com.cst.bigdata.service.integrate.CstStreamHourNoDelayIntegrateService;
import com.cst.stream.common.HBaseTable;
import com.cst.stream.common.HbaseColumn;
import com.cst.stream.common.StreamRedisConstants;
import com.cst.stream.stathour.CstStreamBaseResult;
import com.cst.stream.stathour.voltage.VoltageHourTransfor;
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
@RequestMapping("/stream/hour/statistics/voltage")
@Api(description = "voltage小时数据的查询和存储")
@Slf4j
public class VoltageHourStatisticsController {
    @Autowired
    private CstStreamHourIntegrateService cstStreamHourIntegrateService;

    @Autowired
    private CstStreamHourNoDelayIntegrateService cstStreamHourNoDelayIntegrateService;


    //通过carId 时间戳获取 Voltage  hour data
    @GetMapping(value="/find/{carId}/{time}")
    @ResponseBody
    @ApiOperation(value="Voltage  小时数据查询",httpMethod="GET")
    public CstStreamBaseResult<VoltageHourTransfor> getCstStreamVoltageHourTransforByTimestamp(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间戳", required = true)@PathVariable @NotNull Long time){
        log.debug("########################Voltage  get hour  data:{}  , {}",carId, time);
        return cstStreamHourIntegrateService.getHourTransfor(carId, time,HBaseTable.HOUR_STATISTICS.getSeventhFamilyName(),
                HbaseColumn.HourStatisticsCloumn.voltageHourColumns, VoltageHourTransfor.class);
    }
    @GetMapping(value="/findByDateTime/{carId}/{dateTime}")
    @ResponseBody
    @ApiOperation(value="Trace 小时数据查询",httpMethod="GET")
    public CstStreamBaseResult<VoltageHourTransfor> getCstStreamTraceHourTransforDateTime(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间 yyyy-MM-dd HH", required = true)@PathVariable @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd HH") Date dateTime){
        log.debug("########################Trace get hour  data:{}  , {}",carId, dateTime.getTime());
        return cstStreamHourIntegrateService.getHourTransfor(carId, dateTime.getTime(),HBaseTable.HOUR_STATISTICS.getSeventhFamilyName(),
                HbaseColumn.HourStatisticsCloumn.voltageHourColumns, VoltageHourTransfor.class);
    }
    //entity put Voltage hour data
    @PutMapping(value="/save")
    @ResponseBody
    @ApiOperation(value="Voltage 小时数据存储",httpMethod="PUT")
    public CstStreamBaseResult<VoltageHourTransfor> putCstStreamTraceHourTransfor(
            @ApiParam(value = "Voltage 小时计算结果数据", required = true)@RequestBody @NotNull VoltageHourTransfor voltageHourTransfor){
        log.debug("########################gps saving hour data:{}  , {}", voltageHourTransfor.getCarId(), voltageHourTransfor.getTime());
        return cstStreamHourIntegrateService.putHourTransfor(voltageHourTransfor, HBaseTable.HOUR_STATISTICS.getSeventhFamilyName());
    }

    //通过carId 时间戳获取 Voltage hour data
    @GetMapping(value="/nodelay/find/{carId}")
    @ResponseBody
    @ApiOperation(value="Voltage nodelay小时数据查询",httpMethod="GET")
    public CstStreamBaseResult<VoltageHourTransfor> getCstStreamVoltageNoDelayHourTransforByCarId(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId){
        log.debug("########################Voltage  get hour no delay data:{} ",carId);
        return cstStreamHourNoDelayIntegrateService.getHourNoDelayDataHourTransfor(carId, StreamRedisConstants.HourKey.HOUR_VOLTAGE);
    }

}

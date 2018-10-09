package com.cst.bigdata.controller.hoursource;

import com.cst.bigdata.service.integrate.CstStreamHourFirstIntegrateService;
import com.cst.stream.common.HbaseColumn;
import com.cst.stream.common.StreamTypeDefine;
import com.cst.stream.stathour.CstStreamBaseResult;
import com.cst.stream.stathour.voltage.VoltageHourSource;
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
@RequestMapping("/stream/hour/source/voltage")
@Api(description = "voltage第一条数据的查询和存储")
@Slf4j
public class VoltageHourSourceStatisticsController {
    @Autowired
    private CstStreamHourFirstIntegrateService<VoltageHourSource> cstStreamHourFirstIntegrateService;


    //通过carId 时间戳获取 Voltage  hour data
    @GetMapping(value="/find/{carId}/{time}")
    @ResponseBody
    @ApiOperation(value="Voltage  第一条数据查询",httpMethod="GET")
    public CstStreamBaseResult<VoltageHourSource> getCstStreamVoltageHourTransforByTimestamp(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间戳", required = true)@PathVariable @NotNull Long time){
        log.debug("########################Voltage  get hour source  data:{}  , {}",carId, time);
        return cstStreamHourFirstIntegrateService.getHourSource(carId, time, StreamTypeDefine.VOLTAGE_TYPE,
                HbaseColumn.HourSourceColumn.voltageHourColumns, VoltageHourSource.class);
    }
    @GetMapping(value="/findByDateTime/{carId}/{dateTime}")
    @ResponseBody
    @ApiOperation(value="Trace 第一条数据查询",httpMethod="GET")
    public CstStreamBaseResult<VoltageHourSource> getCstStreamTraceHourTransforDateTime(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间 yyyy-MM-dd HH", required = true)@PathVariable @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd HH") Date dateTime){
        log.debug("########################Voltage get hour source data:{}  , {}",carId, dateTime.getTime());
        return cstStreamHourFirstIntegrateService.getHourSource(carId, dateTime.getTime(), StreamTypeDefine.VOLTAGE_TYPE,
                HbaseColumn.HourSourceColumn.voltageHourColumns, VoltageHourSource.class);
    }
    //entity put Voltage hour data
    @PutMapping(value="/save")
    @ResponseBody
    @ApiOperation(value="Voltage 小时第一条数据存储",httpMethod="PUT")
    public CstStreamBaseResult<VoltageHourSource> putCstStreamTraceHourTransfor(
            @ApiParam(value = "Voltage 小时第一条数据存储", required = true)@RequestBody @NotNull VoltageHourSource voltageHourSource){
        log.debug("########################Voltage saving hour source data:{}  , {}", voltageHourSource.getCarId(), voltageHourSource.getTime());
        return cstStreamHourFirstIntegrateService.putHourSource(voltageHourSource,StreamTypeDefine.VOLTAGE_TYPE);
    }

}

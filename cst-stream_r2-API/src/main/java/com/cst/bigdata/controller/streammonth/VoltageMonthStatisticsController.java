package com.cst.bigdata.controller.streammonth;


import com.cst.bigdata.service.integrate.CstStreamMonthIntegrateService;
import com.cst.stream.common.HBaseTable;
import com.cst.stream.common.HbaseColumn;
import com.cst.stream.stathour.CstStreamBaseResult;
import com.cst.stream.stathour.voltage.VoltageMonthTransfor;
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
 * @Description 电瓶电压月数据接口
 * @title
 */
@RestController
@RequestMapping("/stream/month/statistics/voltage")
@Api(description = "voltage月数据的查询以及存储")
@Slf4j
public class VoltageMonthStatisticsController {

    @Autowired
    private CstStreamMonthIntegrateService cstStreamMonthIntegrateService;


    //通过carId 时间戳获取 Voltage  month data
    @GetMapping(value="/find/{carId}/{time}")
    @ResponseBody
    @ApiOperation(value="Voltage  月数据查询",httpMethod="GET")
    public CstStreamBaseResult<VoltageMonthTransfor> getCstStreamVoltageMonthTransforByTimestamp(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间戳", required = true) @PathVariable @NotNull Long time){
        log.debug("########################Voltage  get month data:{}  , {}",carId, time);
        return cstStreamMonthIntegrateService.getMonthTransfor(carId, time,HBaseTable.MONTH_STATISTICS.getSeventhFamilyName(),
                HbaseColumn.MonthStatisticsCloumn.voltageMonthColumns, VoltageMonthTransfor.class);
    }
    @GetMapping(value="/findByDate/{carId}/{date}")
    @ResponseBody
    @ApiOperation(value="Voltage  月数据查询",httpMethod="GET")
    public CstStreamBaseResult<VoltageMonthTransfor> getCstStreamVoltageMonthTransforByDate(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间 yyyy-MM-dd", required = true) @PathVariable @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd") Date date){
        log.debug("########################Voltage get month data:{}  , {}",carId, date.getTime());
        return cstStreamMonthIntegrateService.getMonthTransfor(carId, date.getTime(),HBaseTable.MONTH_STATISTICS.getSeventhFamilyName(),
                HbaseColumn.MonthStatisticsCloumn.voltageMonthColumns, VoltageMonthTransfor.class);
    }
    //entity put Voltage  month data
    @PutMapping(value="/save")
    @ResponseBody
    @ApiOperation(value="Voltage  月数据存储",httpMethod="PUT")
    public CstStreamBaseResult<VoltageMonthTransfor> putCstStreamVoltageMonthTransfor(
            @ApiParam(value = "Voltage  月数据结果", required = true) @RequestBody @NotNull VoltageMonthTransfor voltageMonthTransfor){
        log.debug("##############################obd saving month data:{}  , {}", voltageMonthTransfor.getCarId(), voltageMonthTransfor.getTime());
        return cstStreamMonthIntegrateService.putMonthTransfor(voltageMonthTransfor, HBaseTable.MONTH_STATISTICS.getSeventhFamilyName());
    }

}

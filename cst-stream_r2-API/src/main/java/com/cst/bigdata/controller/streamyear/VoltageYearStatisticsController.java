package com.cst.bigdata.controller.streamyear;


import com.cst.bigdata.service.integrate.CstStreamYearIntegrateService;
import com.cst.stream.common.HBaseTable;
import com.cst.stream.common.HbaseColumn;
import com.cst.stream.stathour.CstStreamBaseResult;
import com.cst.stream.stathour.voltage.VoltageYearTransfor;
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
 * @Description 电瓶电压年数据接口
 * @title
 */
@RestController
@RequestMapping("/stream/year/statistics/voltage")
@Api(description = "voltage年数据的查询以及存储")
@Slf4j
public class VoltageYearStatisticsController {

    @Autowired
    private CstStreamYearIntegrateService cstStreamYearIntegrateService;


    //通过carId 时间戳获取 Voltage  year data
    @GetMapping(value="/find/{carId}/{time}")
    @ResponseBody
    @ApiOperation(value="Voltage  年数据查询",httpMethod="GET")
    public CstStreamBaseResult<VoltageYearTransfor> getCstStreamVoltageYearTransforByTimestamp(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间戳", required = true) @PathVariable @NotNull Long time){
        log.debug("########################Voltage  get year data:{}  , {}",carId, time);
        return cstStreamYearIntegrateService.getYearTransfor(carId, time,HBaseTable.YEAR_STATISTICS.getSeventhFamilyName(),
                HbaseColumn.YearStatisticsCloumn.voltageYearColumns, VoltageYearTransfor.class);
    }
    @GetMapping(value="/findByDate/{carId}/{date}")
    @ResponseBody
    @ApiOperation(value="Voltage  年数据查询",httpMethod="GET")
    public CstStreamBaseResult<VoltageYearTransfor> getCstStreamVoltageYearTransforByDate(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间 yyyy-MM-dd", required = true) @PathVariable @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd") Date date){
        log.debug("########################Voltage get year data:{}  , {}",carId, date.getTime());
        return cstStreamYearIntegrateService.getYearTransfor(carId, date.getTime(),HBaseTable.YEAR_STATISTICS.getSeventhFamilyName(),
                HbaseColumn.YearStatisticsCloumn.voltageYearColumns, VoltageYearTransfor.class);
    }
    //entity put Voltage  year data
    @PutMapping(value="/save")
    @ResponseBody
    @ApiOperation(value="Voltage  年数据存储",httpMethod="PUT")
    public CstStreamBaseResult<VoltageYearTransfor> putCstStreamVoltageYearTransfor(
            @ApiParam(value = "Voltage  年数据结果", required = true) @RequestBody @NotNull VoltageYearTransfor voltageYearTransfor){
        log.debug("##############################obd saving year data:{}  , {}", voltageYearTransfor.getCarId(), voltageYearTransfor.getTime());
        return cstStreamYearIntegrateService.putYearTransfor(voltageYearTransfor, HBaseTable.YEAR_STATISTICS.getSeventhFamilyName());
    }

}

package com.cst.bigdata.controller.streammonth;

import com.cst.bigdata.service.integrate.CstStreamMonthIntegrateService;
import com.cst.stream.common.HBaseTable;
import com.cst.stream.common.HbaseColumn;
import com.cst.stream.stathour.CstStreamBaseResult;
import com.cst.stream.stathour.obd.ObdMonthTransfor;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;

import javax.validation.constraints.NotNull;
import java.util.Date;

import static com.cst.stream.common.BusinessMathUtil.calcAvarageSpeed;
import static com.cst.stream.common.BusinessMathUtil.calcFuelPerHundred;

/**
 * @author Johnney.Chiu
 * create on 2018/4/17 10:54
 * @Description Obd月数据接口
 * @title
 */

@RestController
@RequestMapping("/stream/month/statistics/obd")
@Api(description = "obd月数据的查询以及存储")
@Slf4j
public class ObdMonthStatisticController {

    @Autowired
    private CstStreamMonthIntegrateService cstStreamMonthIntegrateService;



    //通过carId 时间戳获取 obd month data
    @GetMapping(value="/find/{carId}/{time}")
    @ResponseBody
    @ApiOperation(value="obd 月数据查询",httpMethod="GET")
    public CstStreamBaseResult<ObdMonthTransfor> getCstStreamObdMonthTransforByTimestamp(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间戳", required = true) @PathVariable @NotNull Long time){
        log.debug("########################obd get month data:{}  , {}",carId, time);
        CstStreamBaseResult<ObdMonthTransfor> result=  cstStreamMonthIntegrateService.getMonthTransfor(carId, time, HBaseTable.MONTH_STATISTICS.getFirstFamilyName(),
                HbaseColumn.MonthStatisticsCloumn.obdMonthColumns, ObdMonthTransfor.class);
        if (result != null && result.getData() != null) {
            result.getData().setFuelPerHundred(calcFuelPerHundred(result.getData().getFuel(), result.getData().getMileage()));
            result.getData().setAverageSpeed(calcAvarageSpeed(result.getData().getMileage(),result.getData().getDuration()));
        }
        return result;
    }
    @GetMapping(value="/findByDate/{carId}/{date}")
    @ResponseBody
    @ApiOperation(value="obd 月数据查询",httpMethod="GET")
    public CstStreamBaseResult<ObdMonthTransfor> getCstStreamObdMonthTransforByDate(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间 yyyy-MM-dd", required = true) @PathVariable @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd") Date date){
        log.debug("########################obd get month data:{}  , {}",carId, date.getTime());
        CstStreamBaseResult<ObdMonthTransfor> result= cstStreamMonthIntegrateService.getMonthTransfor(carId, date.getTime(),HBaseTable.MONTH_STATISTICS.getFirstFamilyName(),
                HbaseColumn.MonthStatisticsCloumn.obdMonthColumns, ObdMonthTransfor.class);
        if (result != null && result.getData() != null) {
            result.getData().setFuelPerHundred(calcFuelPerHundred(result.getData().getFuel(), result.getData().getMileage()));
            result.getData().setAverageSpeed(calcAvarageSpeed(result.getData().getMileage(),result.getData().getDuration()));

        }
        return result;
    }
    //entity put obd month data
    @PutMapping(value="/save")
    @ResponseBody
    @ApiOperation(value="obd 月数据存储",httpMethod="PUT")
    public CstStreamBaseResult<ObdMonthTransfor> putCstStreamObdMonthTransfor(
            @ApiParam(value = "obd月数据结果", required = true) @RequestBody @NotNull ObdMonthTransfor obdMonthTransfor){
        log.debug("##############################obd saving month data:{}  , {}", obdMonthTransfor.getCarId(), obdMonthTransfor.getTime());
        CstStreamBaseResult<ObdMonthTransfor> result=cstStreamMonthIntegrateService.putMonthTransfor(obdMonthTransfor,HBaseTable.MONTH_STATISTICS.getFirstFamilyName());
        if (result != null && result.getData() != null) {
            result.getData().setFuelPerHundred(calcFuelPerHundred(result.getData().getFuel(), result.getData().getMileage()));
            result.getData().setAverageSpeed(calcAvarageSpeed(result.getData().getMileage(),result.getData().getDuration()));

        }
        return result;
    }



}

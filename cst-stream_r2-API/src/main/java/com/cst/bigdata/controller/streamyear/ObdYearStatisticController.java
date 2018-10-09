package com.cst.bigdata.controller.streamyear;

import com.cst.bigdata.service.integrate.CstStreamYearIntegrateService;
import com.cst.stream.common.HBaseTable;
import com.cst.stream.common.HbaseColumn;
import com.cst.stream.stathour.CstStreamBaseResult;
import com.cst.stream.stathour.obd.ObdYearTransfor;
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
 * @Description Obd年数据接口
 * @title
 */

@RestController
@RequestMapping("/stream/year/statistics/obd")
@Api(description = "obd年数据的查询以及存储")
@Slf4j
public class ObdYearStatisticController {

    @Autowired
    private CstStreamYearIntegrateService cstStreamYearIntegrateService;



    //通过carId 时间戳获取 obd year data
    @GetMapping(value="/find/{carId}/{time}")
    @ResponseBody
    @ApiOperation(value="obd 年数据查询",httpMethod="GET")
    public CstStreamBaseResult<ObdYearTransfor> getCstStreamObdYearTransforByTimestamp(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间戳", required = true) @PathVariable @NotNull Long time){
        log.debug("########################obd get year data:{}  , {}",carId, time);
        CstStreamBaseResult<ObdYearTransfor>  result= cstStreamYearIntegrateService.getYearTransfor(carId, time, HBaseTable.YEAR_STATISTICS.getFirstFamilyName(),
                HbaseColumn.YearStatisticsCloumn.obdYearColumns, ObdYearTransfor.class);
        if (result != null && result.getData() != null) {
            result.getData().setFuelPerHundred(calcFuelPerHundred(result.getData().getFuel(), result.getData().getMileage()));
            result.getData().setAverageSpeed(calcAvarageSpeed(result.getData().getMileage(),result.getData().getDuration()));

        }
        return result;
    }
    @GetMapping(value="/findByDate/{carId}/{date}")
    @ResponseBody
    @ApiOperation(value="obd 年数据查询",httpMethod="GET")
    public CstStreamBaseResult<ObdYearTransfor> getCstStreamObdYearTransforByDate(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间 yyyy-MM-dd", required = true) @PathVariable @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd") Date date){
        log.debug("########################obd get year data:{}  , {}",carId, date.getTime());
        CstStreamBaseResult<ObdYearTransfor> result= cstStreamYearIntegrateService.getYearTransfor(carId, date.getTime(),HBaseTable.YEAR_STATISTICS.getFirstFamilyName(),
                HbaseColumn.YearStatisticsCloumn.obdYearColumns, ObdYearTransfor.class);
        if (result != null && result.getData() != null) {
            result.getData().setFuelPerHundred(calcFuelPerHundred(result.getData().getFuel(), result.getData().getMileage()));
            result.getData().setAverageSpeed(calcAvarageSpeed(result.getData().getMileage(),result.getData().getDuration()));

        }
        return result;
    }
    //entity put obd year data
    @PutMapping(value="/save")
    @ResponseBody
    @ApiOperation(value="obd 年数据存储",httpMethod="PUT")
    public CstStreamBaseResult<ObdYearTransfor> putCstStreamObdYearTransfor(
            @ApiParam(value = "obd年数据结果", required = true) @RequestBody @NotNull ObdYearTransfor obdYearTransfor){
        log.debug("##############################obd saving year data:{}  , {}", obdYearTransfor.getCarId(), obdYearTransfor.getTime());
        CstStreamBaseResult<ObdYearTransfor> result= cstStreamYearIntegrateService.putYearTransfor(obdYearTransfor,HBaseTable.YEAR_STATISTICS.getFirstFamilyName());
        if (result != null && result.getData() != null) {
            result.getData().setFuelPerHundred(calcFuelPerHundred(result.getData().getFuel(), result.getData().getMileage()));
            result.getData().setAverageSpeed(calcAvarageSpeed(result.getData().getMileage(),result.getData().getDuration()));

        }
        return result;
    }



}

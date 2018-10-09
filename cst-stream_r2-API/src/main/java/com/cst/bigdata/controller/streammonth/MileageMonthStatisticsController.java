package com.cst.bigdata.controller.streammonth;

import com.cst.bigdata.service.integrate.CstStreamMonthIntegrateService;
import com.cst.stream.common.HBaseTable;
import com.cst.stream.common.HbaseColumn;
import com.cst.stream.stathour.CstStreamBaseResult;
import com.cst.stream.stathour.mileage.MileageMonthTransfor;
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
 * create on 2018/4/17 10:56
 * @Description De月数据接口
 * @title
 */

@RestController
@RequestMapping("/stream/month/statistics/mileage")
@Api(description = "de月数据的查询以及存储")
@Slf4j
public class MileageMonthStatisticsController {

    @Autowired
    private CstStreamMonthIntegrateService cstStreamMonthIntegrateService;


    //通过carId 时间戳获取 mileage Month data
    @GetMapping(value="/find/{carId}/{time}")
    @ResponseBody
    @ApiOperation(value="mileage 月数据查询",httpMethod="GET")
    public CstStreamBaseResult<MileageMonthTransfor> getCstStreamDeMonthTransforByTimestamp(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间戳", required = true) @PathVariable @NotNull Long time){
        log.debug("########################mileage find Month data:{}  , {}",carId, time);
        return cstStreamMonthIntegrateService.getMonthTransfor(carId, time,HBaseTable.MONTH_STATISTICS.getEighthFamilyName(),
                HbaseColumn.MonthStatisticsCloumn.mileageMonthColumns, MileageMonthTransfor.class);
    }
    @GetMapping(value="/findByDate/{carId}/{month}")
    @ResponseBody
    @ApiOperation(value="mileage 月数据查询",httpMethod="GET")
    public CstStreamBaseResult<MileageMonthTransfor> getCstStreamDeMonthTransforByDate(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间 yyyy-MM-dd", required = true) @PathVariable @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd") Date month){
        log.debug("########################mileage finding month data:{}  , {}",carId, month.getTime());
        return cstStreamMonthIntegrateService.getMonthTransfor(carId, month.getTime(),HBaseTable.MONTH_STATISTICS.getEighthFamilyName(),
                HbaseColumn.MonthStatisticsCloumn.mileageMonthColumns, MileageMonthTransfor.class);
    }
    //entity put mileage month data
    @PutMapping(value="/save")
    @ResponseBody
    @ApiOperation(value="mileage 月数据存储",httpMethod="PUT")
    public CstStreamBaseResult<MileageMonthTransfor> putCstStreamDeMonthTransfor(
            @ApiParam(value = "mileage月数据结果", required = true) @RequestBody @NotNull MileageMonthTransfor mileageMonthTransfor){
        log.debug("########################mileage saving month  data:{}  , {}", mileageMonthTransfor.getCarId(), mileageMonthTransfor.getTime());
        return cstStreamMonthIntegrateService.putMonthTransfor(mileageMonthTransfor, HBaseTable.MONTH_STATISTICS.getEighthFamilyName());
    }


}

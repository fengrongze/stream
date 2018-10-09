package com.cst.bigdata.controller.streammonth;

import com.cst.bigdata.service.integrate.CstStreamMonthIntegrateService;
import com.cst.stream.common.HBaseTable;
import com.cst.stream.common.HbaseColumn;
import com.cst.stream.stathour.CstStreamBaseResult;
import com.cst.stream.stathour.de.DeMonthTransfor;
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
@RequestMapping("/stream/month/statistics/de")
@Api(description = "de月数据的查询以及存储")
@Slf4j
public class DeMonthStatisticsController {

    @Autowired
    private CstStreamMonthIntegrateService cstStreamMonthIntegrateService;


    //通过carId 时间戳获取 de Month data
    @GetMapping(value="/find/{carId}/{time}")
    @ResponseBody
    @ApiOperation(value="de 月数据查询",httpMethod="GET")
    public CstStreamBaseResult<DeMonthTransfor> getCstStreamDeMonthTransforByTimestamp(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间戳", required = true) @PathVariable @NotNull Long time){
        log.debug("########################de find Month data:{}  , {}",carId, time);
        return cstStreamMonthIntegrateService.getMonthTransfor(carId, time,HBaseTable.MONTH_STATISTICS.getFourthFamilyName(),
                HbaseColumn.MonthStatisticsCloumn.deMonthColumns, DeMonthTransfor.class);
    }
    @GetMapping(value="/findByDate/{carId}/{month}")
    @ResponseBody
    @ApiOperation(value="de 月数据查询",httpMethod="GET")
    public CstStreamBaseResult<DeMonthTransfor> getCstStreamDeMonthTransforByDate(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间 yyyy-MM-dd", required = true) @PathVariable @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd") Date month){
        log.debug("########################de finding month data:{}  , {}",carId, month.getTime());
        return cstStreamMonthIntegrateService.getMonthTransfor(carId, month.getTime(),HBaseTable.MONTH_STATISTICS.getFourthFamilyName(),
                HbaseColumn.MonthStatisticsCloumn.deMonthColumns, DeMonthTransfor.class);
    }
    //entity put de month data
    @PutMapping(value="/save")
    @ResponseBody
    @ApiOperation(value="de 月数据存储",httpMethod="PUT")
    public CstStreamBaseResult<DeMonthTransfor> putCstStreamDeMonthTransfor(
            @ApiParam(value = "de月数据结果", required = true) @RequestBody @NotNull DeMonthTransfor deMonthTransfor){
        log.debug("########################de saving month  data:{}  , {}", deMonthTransfor.getCarId(), deMonthTransfor.getTime());
        return cstStreamMonthIntegrateService.putMonthTransfor(deMonthTransfor, HBaseTable.MONTH_STATISTICS.getFourthFamilyName());
    }


}

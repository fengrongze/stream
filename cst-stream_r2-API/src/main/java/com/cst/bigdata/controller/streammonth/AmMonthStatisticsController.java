package com.cst.bigdata.controller.streammonth;

import com.cst.bigdata.service.integrate.CstStreamMonthIntegrateService;
import com.cst.stream.common.HBaseTable;
import com.cst.stream.common.HbaseColumn;
import com.cst.stream.stathour.CstStreamBaseResult;
import com.cst.stream.stathour.am.AmMonthTransfor;
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
 * @Description am月数据接口
 * @title
 */
@RestController
@RequestMapping("/stream/month/statistics/am")
@Api(description = "am月数据的查询以及存储")
@Slf4j
public class AmMonthStatisticsController {

    @Autowired
    private CstStreamMonthIntegrateService cstStreamMonthIntegrateService;


    @GetMapping(value="/find/{carId}/{time}")
    @ResponseBody
    @ApiOperation(value="am 月数据查询",httpMethod="GET")
    public CstStreamBaseResult<AmMonthTransfor> getCstStreamAmMonthTransforByTimestamp(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间戳", required = true) @PathVariable @NotNull Long time){
        log.debug("########################am get month data:{}  , {}",carId, time);
        return cstStreamMonthIntegrateService.getMonthTransfor(carId, time,HBaseTable.MONTH_STATISTICS.getThirdFamilyName(),
                HbaseColumn.MonthStatisticsCloumn.amMonthColumns, AmMonthTransfor.class);
    }
    @GetMapping(value="/findByDate/{carId}/{month}")
    @ResponseBody
    @ApiOperation(value="am 月数据查询",httpMethod="GET")
    public CstStreamBaseResult<AmMonthTransfor> getCstStreamAmMonthTransforByDate(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间 yyyy-MM-dd", required = true) @PathVariable @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd") Date month){
        log.debug("########################am get month data:{}  , {}",carId, month.getTime());
        return cstStreamMonthIntegrateService.getMonthTransfor(carId,  month.getTime(), HBaseTable.MONTH_STATISTICS.getThirdFamilyName(),
                HbaseColumn.MonthStatisticsCloumn.amMonthColumns, AmMonthTransfor.class);
    }
    @PutMapping(value="/save")
    @ResponseBody
    @ApiOperation(value="am 月数据存储",httpMethod="PUT")
    public CstStreamBaseResult<AmMonthTransfor> putCstStreamAmMonthTransfor(
            @ApiParam(value = "am月数据结果", required = true)  @RequestBody @NotNull AmMonthTransfor amMonthTransfor){
        log.debug("########################am saving data data:{}  , {}", amMonthTransfor.getCarId(), amMonthTransfor.getTime());
        return cstStreamMonthIntegrateService.putMonthTransfor(amMonthTransfor,HBaseTable.MONTH_STATISTICS.getThirdFamilyName());
    }


}

package com.cst.bigdata.controller.streamyear;

import com.cst.bigdata.service.integrate.CstStreamYearIntegrateService;
import com.cst.stream.common.HBaseTable;
import com.cst.stream.common.HbaseColumn;
import com.cst.stream.stathour.CstStreamBaseResult;
import com.cst.stream.stathour.am.AmYearTransfor;
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
 * @Description am天数据接口
 * @title
 */
@RestController
@RequestMapping("/stream/year/statistics/am")
@Api(description = "am天数据的查询以及存储")
@Slf4j
public class AmYearStatisticsController {

    @Autowired
    private CstStreamYearIntegrateService cstStreamYearIntegrateService;


    @GetMapping(value="/find/{carId}/{time}")
    @ResponseBody
    @ApiOperation(value="am 天数据查询",httpMethod="GET")
    public CstStreamBaseResult<AmYearTransfor> getCstStreamAmYearTransforByTimestamp(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间戳", required = true) @PathVariable @NotNull Long time){
        log.debug("########################am get year data:{}  , {}",carId, time);
        return cstStreamYearIntegrateService.getYearTransfor(carId, time,HBaseTable.YEAR_STATISTICS.getThirdFamilyName(),
                HbaseColumn.YearStatisticsCloumn.amYearColumns, AmYearTransfor.class);
    }
    @GetMapping(value="/findByDate/{carId}/{year}")
    @ResponseBody
    @ApiOperation(value="am 天数据查询",httpMethod="GET")
    public CstStreamBaseResult<AmYearTransfor> getCstStreamAmYearTransforByDate(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间 yyyy-MM-dd", required = true) @PathVariable @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd") Date year){
        log.debug("########################am get year data:{}  , {}",carId, year.getTime());
        return cstStreamYearIntegrateService.getYearTransfor(carId,  year.getTime(), HBaseTable.YEAR_STATISTICS.getThirdFamilyName(),
                HbaseColumn.YearStatisticsCloumn.amYearColumns, AmYearTransfor.class);
    }
    @PutMapping(value="/save")
    @ResponseBody
    @ApiOperation(value="am 天数据存储",httpMethod="PUT")
    public CstStreamBaseResult<AmYearTransfor> putCstStreamAmYearTransfor(
            @ApiParam(value = "am天数据结果", required = true)  @RequestBody @NotNull AmYearTransfor amYearTransfor){
        log.debug("########################am saving data data:{}  , {}", amYearTransfor.getCarId(), amYearTransfor.getTime());
        return cstStreamYearIntegrateService.putYearTransfor(amYearTransfor,HBaseTable.YEAR_STATISTICS.getThirdFamilyName());
    }


}

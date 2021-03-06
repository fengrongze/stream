package com.cst.bigdata.controller.streamhour;

import com.cst.bigdata.service.integrate.CstStreamHourIntegrateService;
import com.cst.bigdata.service.integrate.CstStreamHourNoDelayIntegrateService;
import com.cst.stream.common.HBaseTable;
import com.cst.stream.common.HbaseColumn;
import com.cst.stream.common.StreamRedisConstants;
import com.cst.stream.common.StreamTypeDefine;
import com.cst.stream.stathour.CstStreamBaseResult;
import com.cst.stream.stathour.am.AmHourTransfor;
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
 * @Description am小时数据接口
 * @title
 */
@RestController
@RequestMapping("/stream/hour/statistics/am")
@Api(description = "am小时数据的查询和存储")
@Slf4j
public class AmHourStatisticsController {

    @Autowired
    private CstStreamHourIntegrateService cstStreamHourIntegrateService;

    @Autowired
    private CstStreamHourNoDelayIntegrateService cstStreamHourNoDelayIntegrateService;

    //通过carId 时间戳获取 am hour data
    @GetMapping(value="/find/{carId}/{time}")
    @ResponseBody
    @ApiOperation(value="am 小时数据查询",httpMethod="GET")
    public CstStreamBaseResult<AmHourTransfor> getCstStreamAmHourTransforByTimestamp(
            @ApiParam(value = "车id", required = true) @PathVariable("carId") @NotNull String carId,
            @ApiParam(value = "时间戳", required = true) @PathVariable("time") @NotNull Long time){
        log.debug("########################am get hour data:{}  , {}",carId, time);
       // return cstStreamHourIntegrateService.getAmHourTransfor(carId, time);
        return cstStreamHourIntegrateService.getHourTransfor(carId, time,HBaseTable.HOUR_STATISTICS.getThirdFamilyName(),
                HbaseColumn.HourStatisticsCloumn.amHourColumns, AmHourTransfor.class);
    }
    @GetMapping(value="/findByDateTime/{carId}/{dateTime}")
    @ResponseBody
    @ApiOperation(value="am 小时数据查询",httpMethod="GET")
    public CstStreamBaseResult<AmHourTransfor> getCstStreamAmHourTransforByDateTime(
            @ApiParam(value = "车id", required = true) @PathVariable("carId") @NotNull String carId,
            @ApiParam(value = "时间 yyyy-MM-dd HH", required = true) @PathVariable("dateTime") @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd HH") Date dateTime){
        log.debug("########################am get hour data:{}  , {}",carId, dateTime.getTime());
        return cstStreamHourIntegrateService.getHourTransfor(carId, dateTime.getTime(), HBaseTable.HOUR_STATISTICS.getThirdFamilyName(),
                HbaseColumn.HourStatisticsCloumn.amHourColumns, AmHourTransfor.class);
    }
    //entity put am hour data
    @PutMapping(value="/save")
    @ResponseBody
    @ApiOperation(value="am 小时数据存储",httpMethod="PUT")
    public CstStreamBaseResult<AmHourTransfor> putCstStreamAmHourTransfor(
            @ApiParam(value = "am 小时计算结果数据", required = true)@RequestBody @NotNull AmHourTransfor amHourTransfor){
        log.debug("########################am saving hour data:{}  , {}", amHourTransfor.getCarId(), amHourTransfor.getTime());
        return cstStreamHourIntegrateService.putHourTransfor(amHourTransfor,HBaseTable.HOUR_STATISTICS.getThirdFamilyName());
    }

    //通过carId 获取 am hour data
    @GetMapping(value="/nodelay/find/{carId}")
    @ResponseBody
    @ApiOperation(value="am 小时数据查询",httpMethod="GET")
    public CstStreamBaseResult<AmHourTransfor> getCstStreamAmNoDelayHourTransforByCarId(
            @ApiParam(value = "车id", required = true) @PathVariable("carId") @NotNull String carId){
        log.debug("########################am get hour data:{} ",carId);
        return cstStreamHourNoDelayIntegrateService.getAmNoDelayHourTransfor(carId, StreamRedisConstants.HourKey.HOUR_AM);
    }


}

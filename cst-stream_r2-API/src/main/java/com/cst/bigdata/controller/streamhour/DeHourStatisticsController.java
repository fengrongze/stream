package com.cst.bigdata.controller.streamhour;

import com.cst.bigdata.service.integrate.CstStreamHourIntegrateService;
import com.cst.bigdata.service.integrate.CstStreamHourNoDelayIntegrateService;
import com.cst.stream.common.HBaseTable;
import com.cst.stream.common.HbaseColumn;
import com.cst.stream.common.StreamRedisConstants;
import com.cst.stream.common.StreamTypeDefine;
import com.cst.stream.stathour.CstStreamBaseResult;
import com.cst.stream.stathour.de.DeHourSource;
import com.cst.stream.stathour.de.DeHourTransfor;
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
 * @Description De小时数据接口
 * @title
 */
@RestController
@RequestMapping("/stream/hour/statistics/de")
@Api(description = "de小时数据的查询和存储")
@Slf4j
public class DeHourStatisticsController {
    @Autowired
    private CstStreamHourIntegrateService cstStreamHourIntegrateService;

    @Autowired
    private CstStreamHourNoDelayIntegrateService cstStreamHourNoDelayIntegrateService;

    //通过carId 时间戳获取 de hour data
    @GetMapping(value="/find/{carId}/{time}")
    @ResponseBody
    @ApiOperation(value="de nodelay小时数据查询",httpMethod="GET")
    public CstStreamBaseResult<DeHourTransfor> getCstStreamDeHourTransforByTimestamp(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间戳", required = true)@PathVariable @NotNull Long time){
        log.debug("########################de saving hour data:{}  , {}",carId, time);
        return cstStreamHourIntegrateService.getHourTransfor(carId, time, HBaseTable.HOUR_STATISTICS.getFourthFamilyName(),
                HbaseColumn.HourStatisticsCloumn.deHourColumns, DeHourTransfor.class);
    }
    @GetMapping(value="/findByDateTime/{carId}/{dateTime}")
    @ResponseBody
    @ApiOperation(value="de 小时数据查询",httpMethod="GET")
    public CstStreamBaseResult<DeHourTransfor> getCstStreamDeHourTransforByDateTime(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间 yyyy-MM-dd HH", required = true)@PathVariable @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd HH") Date dateTime){
        log.debug("########################de saving hour data:{}  , {}",carId, dateTime.getTime());
        return cstStreamHourIntegrateService.getHourTransfor(carId, dateTime.getTime(),HBaseTable.HOUR_STATISTICS.getFourthFamilyName(),
                HbaseColumn.HourStatisticsCloumn.deHourColumns, DeHourTransfor.class);
    }
    //entity put de hour data
    @PutMapping(value="/save")
    @ResponseBody
    @ApiOperation(value="de 小时数据存储",httpMethod="PUT")
    public CstStreamBaseResult<DeHourTransfor> putCstStreamDeHourTransfor(
            @ApiParam(value = "de 小时计算结果数据", required = true) @RequestBody @NotNull DeHourTransfor deHourTransfor){
        log.debug("########################de saving hour  data:{}  , {}", deHourTransfor.getCarId(), deHourTransfor.getTime());
        return cstStreamHourIntegrateService.putHourTransfor(deHourTransfor,HBaseTable.HOUR_STATISTICS.getFourthFamilyName());
    }
    //通过carId 获取 de hour data
    @GetMapping(value="/find/nodelay/{carId}")
    @ResponseBody
    @ApiOperation(value="de nodelay小时数据查询",httpMethod="GET")
    public CstStreamBaseResult<DeHourTransfor> getCstStreamDeNoDelayHourTransforByCarId(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId){
        log.debug("########################de saving hour data:{}  ",carId);
        return cstStreamHourNoDelayIntegrateService
                .getDeNoDelayHourTransfor(carId, StreamTypeDefine.DE_TYPE,
                StreamRedisConstants.HourKey.HOUR_DE, HbaseColumn.HourSourceColumn.deHourColumns, DeHourSource.class);
    }

}

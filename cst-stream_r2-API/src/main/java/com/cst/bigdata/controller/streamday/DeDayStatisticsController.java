package com.cst.bigdata.controller.streamday;

import com.cst.bigdata.service.integrate.CstStreamDayIntegrateService;
import com.cst.bigdata.service.integrate.CstStreamDayNoDelayIntegrateService;
import com.cst.stream.common.HBaseTable;
import com.cst.stream.common.HbaseColumn;
import com.cst.stream.common.StreamRedisConstants;
import com.cst.stream.common.StreamTypeDefine;
import com.cst.stream.stathour.CstStreamBaseResult;
import com.cst.stream.stathour.de.DeDayTransfor;
import com.cst.stream.stathour.de.DeHourSource;
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
 * @Description De天数据接口
 * @title
 */

@RestController
@RequestMapping("/stream/day/statistics/de")
@Api(description = "de天数据的查询以及存储")
@Slf4j
public class DeDayStatisticsController {

    @Autowired
    private CstStreamDayIntegrateService cstStreamDayIntegrateService;

    @Autowired
    private CstStreamDayNoDelayIntegrateService cstStreamDayNoDelayIntegrateService;

    //通过carId 时间戳获取 de day data
    @GetMapping(value="/find/{carId}/{time}")
    @ResponseBody
    @ApiOperation(value="de 天数据查询",httpMethod="GET")
    public CstStreamBaseResult<DeDayTransfor> getCstStreamDeDayTransforByTimestamp(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间戳", required = true) @PathVariable @NotNull Long time){
        log.debug("########################de find day data:{}  , {}",carId, time);
        return cstStreamDayIntegrateService.getDayTransfor(carId, time,HBaseTable.DAY_STATISTICS.getFourthFamilyName(),
                HbaseColumn.DayStatisticsCloumn.deDayColumns, DeDayTransfor.class);
    }
    @GetMapping(value="/findByDate/{carId}/{date}")
    @ResponseBody
    @ApiOperation(value="de 天数据查询",httpMethod="GET")
    public CstStreamBaseResult<DeDayTransfor> getCstStreamDeDayTransforByDate(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间 yyyy-MM-dd", required = true) @PathVariable @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd") Date date){
        log.debug("########################de finding day data:{}  , {}",carId, date.getTime());
        return cstStreamDayIntegrateService.getDayTransfor(carId, date.getTime(),HBaseTable.DAY_STATISTICS.getFourthFamilyName(),
                HbaseColumn.DayStatisticsCloumn.deDayColumns, DeDayTransfor.class);
    }
    //entity put de day data
    @PutMapping(value="/save")
    @ResponseBody
    @ApiOperation(value="de 天数据存储",httpMethod="PUT")
    public CstStreamBaseResult<DeDayTransfor> putCstStreamDeDayTransfor(
            @ApiParam(value = "de天数据结果", required = true) @RequestBody @NotNull DeDayTransfor deDayTransfor){
        log.debug("########################de saving day  data:{}  , {}", deDayTransfor.getCarId(), deDayTransfor.getTime());
        return cstStreamDayIntegrateService.putDayTransfor(deDayTransfor, HBaseTable.DAY_STATISTICS.getFourthFamilyName());
    }

    @GetMapping(value="/nodelay/find/{carId}")
    @ResponseBody
    @ApiOperation(value="de nodelay  天数据查询",httpMethod="GET")
    public CstStreamBaseResult<DeDayTransfor> getCstStreamDeNoDelayDayTransforByCarId(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId){
        log.debug("########################de finding day data:{} ",carId);
        return cstStreamDayNoDelayIntegrateService.getDeNoDelayDayTransfor(carId, StreamTypeDefine.DE_TYPE,
                StreamRedisConstants.DayKey.DAY_DE, HbaseColumn.DaySourceColumn.deDayColumns, DeDayTransfor.class);
    }

}

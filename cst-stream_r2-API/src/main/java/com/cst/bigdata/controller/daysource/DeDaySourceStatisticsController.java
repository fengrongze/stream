package com.cst.bigdata.controller.daysource;

import com.cst.bigdata.service.integrate.CstStreamDayFirstIntegrateService;
import com.cst.stream.common.HbaseColumn;
import com.cst.stream.common.StreamTypeDefine;
import com.cst.stream.stathour.CstStreamBaseResult;
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
@RequestMapping("/stream/day/source/de")
@Api(description = "de第一条数据的查询和存储")
@Slf4j
public class DeDaySourceStatisticsController {

    @Autowired
    private CstStreamDayFirstIntegrateService<DeHourSource> cstStreamDayFirstIntegrateService;


    //通过carId 时间戳获取 de day data
    @GetMapping(value="/find/{carId}/{time}")
    @ResponseBody
    @ApiOperation(value="de 第一条数据查询",httpMethod="GET")
    public CstStreamBaseResult<DeHourSource> getCstStreamDeHourSourceByTimestamp(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间戳", required = true)@PathVariable @NotNull Long time){
        log.debug("########################de saving day source data:{}  , {}",carId, time);
        return cstStreamDayFirstIntegrateService.getDaySource(carId, time, StreamTypeDefine.DE_TYPE,
                HbaseColumn.DaySourceColumn.deDayColumns, DeHourSource.class);
    }
    @GetMapping(value="/findByDateTime/{carId}/{dateTime}")
    @ResponseBody
    @ApiOperation(value="de 第一条数据查询",httpMethod="GET")
    public CstStreamBaseResult<DeHourSource> getCstStreamDeHourSourceByDateTime(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间 yyyy-MM-dd", required = true)@PathVariable @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd") Date dateTime){
        log.debug("########################de saving day source data:{}  , {}",carId, dateTime.getTime());
        return cstStreamDayFirstIntegrateService.getDaySource(carId, dateTime.getTime(), StreamTypeDefine.DE_TYPE,
                HbaseColumn.HourSourceColumn.deHourColumns, DeHourSource.class);
    }
    //entity put de day data
    @PutMapping(value="/save")
    @ResponseBody
    @ApiOperation(value="de 小时第一条数据存储",httpMethod="PUT")
    public CstStreamBaseResult<DeHourSource> putCstStreamDeHourSource(
            @ApiParam(value = "de 小时第一条数据存储", required = true) @RequestBody @NotNull DeHourSource deHourSource){
        log.debug("########################de saving day  data:{}  , {}", deHourSource.getCarId(), deHourSource.getTime());
        return cstStreamDayFirstIntegrateService.putDaySource(deHourSource,StreamTypeDefine.DE_TYPE);
    }

}

package com.cst.bigdata.controller.daysource;

import com.cst.bigdata.service.integrate.CstStreamDayFirstIntegrateService;
import com.cst.stream.common.HbaseColumn;
import com.cst.stream.common.StreamTypeDefine;
import com.cst.stream.stathour.CstStreamBaseResult;
import com.cst.stream.stathour.obd.ObdHourSource;
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
 * @Description Obd天数据接口
 * @title
 */
@RestController
@RequestMapping("/stream/day/source/obd")
@Api(description = "obd第一条数据的查询和存储")
@Slf4j
public class ObdDaySourceStatisticController {

    @Autowired
    private CstStreamDayFirstIntegrateService<ObdHourSource> cstStreamDayFirstIntegrateService;



    //通过carId 时间戳获取 obd day data
    @GetMapping(value="/find/{carId}/{time}")
    @ResponseBody
    @ApiOperation(value="obd 第一条数据查询",httpMethod="GET")
    public CstStreamBaseResult<ObdHourSource> getCstStreamObdHourTransforByTimestamp(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间戳", required = true)@PathVariable @NotNull Long time){
        log.debug("########################obd get day source data:{}  , {}",carId, time);
        return cstStreamDayFirstIntegrateService.getDaySource(carId, time, StreamTypeDefine.OBD_TYPE,
                HbaseColumn.DaySourceColumn.obdDayColumns, ObdHourSource.class);
    }
    @GetMapping(value="/findByDateTime/{carId}/{dateTime}")
    @ResponseBody
    @ApiOperation(value="obd 第一条数据查询",httpMethod="GET")
    public CstStreamBaseResult<ObdHourSource> getCstStreamObdHourTransforByDateTime(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间 yyyy-MM-dd", required = true)@PathVariable @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd") Date dateTime){
        log.debug("########################obd get day source data:{}  , {}",carId, dateTime.getTime());
        return cstStreamDayFirstIntegrateService.getDaySource(carId, dateTime.getTime(), StreamTypeDefine.OBD_TYPE,
                HbaseColumn.DaySourceColumn.obdDayColumns, ObdHourSource.class);
    }
    //entity put obd hour data
    @PutMapping(value="/save")
    @ResponseBody
    @ApiOperation(value="obd 小时第一条数据存储",httpMethod="PUT")
    public CstStreamBaseResult<ObdHourSource> putCstStreamGpsHourTransfor(
            @ApiParam(value = "obd 小时第一条数据存储", required = true)@RequestBody @NotNull ObdHourSource obdHourSource){
        log.debug("##############################obd saving day source data:{}  , {}", obdHourSource.getCarId(), obdHourSource.getTime());
        return cstStreamDayFirstIntegrateService.putDaySource(obdHourSource,StreamTypeDefine.OBD_TYPE);
    }



}

package com.cst.bigdata.controller.daysource;

import com.cst.bigdata.service.integrate.CstStreamDayFirstIntegrateService;
import com.cst.stream.common.HbaseColumn;
import com.cst.stream.common.StreamTypeDefine;
import com.cst.stream.stathour.CstStreamBaseResult;
import com.cst.stream.stathour.am.AmHourSource;
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
@RequestMapping("/stream/day/source/am")
@Api(description = "am第一条数据的查询和存储")
@Slf4j
public class AmDaySourceStatisticsController {

    @Autowired
    private CstStreamDayFirstIntegrateService<AmHourSource> cstStreamDayFirstIntegrateService;


    @GetMapping(value="/find/{carId}/{time}")
    @ResponseBody
    @ApiOperation(value="am 第一条数据查询",httpMethod="GET")
    public CstStreamBaseResult<AmHourSource> getCstStreamAmHourSourceByTimestamp(
            @ApiParam(value = "车id", required = true) @PathVariable("carId") @NotNull String carId,
            @ApiParam(value = "时间戳", required = true) @PathVariable("time") @NotNull Long time){
        log.debug("########################am get day data:{}  , {}",carId, time);
        return cstStreamDayFirstIntegrateService.getDaySource(carId, time, StreamTypeDefine.AM_TYPE,
                HbaseColumn.DaySourceColumn.amDayColumns, AmHourSource.class);
    }
    @GetMapping(value="/findByDateTime/{carId}/{dateTime}")
    @ResponseBody
    @ApiOperation(value="am 第一条数据查询",httpMethod="GET")
    public CstStreamBaseResult<AmHourSource> getCstStreamAmHourSourceByDateTime(
            @ApiParam(value = "车id", required = true) @PathVariable("carId") @NotNull String carId,
            @ApiParam(value = "时间 yyyy-MM-dd", required = true) @PathVariable("dateTime") @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd") Date dateTime){
        log.debug("########################am get day source data:{}  , {}",carId, dateTime.getTime());
        return cstStreamDayFirstIntegrateService.getDaySource(carId, dateTime.getTime(), StreamTypeDefine.AM_TYPE,
                HbaseColumn.DaySourceColumn.amDayColumns, AmHourSource.class);
    }
    //entity put am hour data
    @PutMapping(value="/save")
    @ResponseBody
    @ApiOperation(value="am 小时第一条数据存储",httpMethod="PUT")
    public CstStreamBaseResult<AmHourSource> putCstStreamAmHourSource(
            @ApiParam(value = "am 小时第一条数据存储", required = true)@RequestBody @NotNull AmHourSource amHourSource){
        log.debug("########################am saving day source data:{}  , {}", amHourSource.getCarId(), amHourSource.getTime());
        return cstStreamDayFirstIntegrateService.putDaySource(amHourSource,StreamTypeDefine.AM_TYPE);
    }


}

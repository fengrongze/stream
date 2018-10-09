package com.cst.bigdata.controller.streamday;

import com.cst.bigdata.service.integrate.CstStreamDayIntegrateService;
import com.cst.bigdata.service.integrate.CstStreamDayNoDelayIntegrateService;
import com.cst.stream.common.HBaseTable;
import com.cst.stream.common.HbaseColumn;
import com.cst.stream.common.StreamRedisConstants;
import com.cst.stream.common.StreamTypeDefine;
import com.cst.stream.stathour.CstStreamBaseResult;
import com.cst.stream.stathour.de.DeHourSource;
import com.cst.stream.stathour.obd.ObdDayTransfor;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;

import javax.validation.constraints.NotNull;
import java.util.Date;

import static com.cst.stream.common.BusinessMathUtil.calcAvarageSpeed;
import static com.cst.stream.common.BusinessMathUtil.calcFuelPerHundred;

/**
 * @author Johnney.Chiu
 * create on 2018/4/17 10:54
 * @Description Obd天数据接口
 * @title
 */

@RestController
@RequestMapping("/stream/day/statistics/obd")
@Api(description = "obd天数据的查询以及存储")
@Slf4j
public class ObdDayStatisticController {

    @Autowired
    private CstStreamDayIntegrateService cstStreamDayIntegrateService;

    @Autowired
    private CstStreamDayNoDelayIntegrateService cstStreamDayNoDelayIntegrateService;



    //通过carId 时间戳获取 obd day data
    @GetMapping(value="/find/{carId}/{time}")
    @ResponseBody
    @ApiOperation(value="obd 天数据查询",httpMethod="GET")
    public CstStreamBaseResult<ObdDayTransfor> getCstStreamObdDayTransforByTimestamp(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间戳", required = true) @PathVariable @NotNull Long time){
        log.debug("########################obd get day data:{}  , {}",carId, time);
        CstStreamBaseResult<ObdDayTransfor> result= cstStreamDayIntegrateService.getDayTransfor(carId, time, HBaseTable.DAY_STATISTICS.getFirstFamilyName(),
                HbaseColumn.DayStatisticsCloumn.obdDayColumns, ObdDayTransfor.class);
        if (result != null && result.getData() != null) {
            result.getData().setFuelPerHundred(calcFuelPerHundred(result.getData().getFuel(), result.getData().getMileage()));
            result.getData().setAverageSpeed(calcAvarageSpeed(result.getData().getMileage(),result.getData().getDuration()));

        }
        return result;
    }
    @GetMapping(value="/findByDate/{carId}/{date}")
    @ResponseBody
    @ApiOperation(value="obd 天数据查询",httpMethod="GET")
    public CstStreamBaseResult<ObdDayTransfor> getCstStreamObdDayTransforByDate(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间 yyyy-MM-dd", required = true) @PathVariable @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd") Date date){
        log.debug("########################obd get day data:{}  , {}",carId, date.getTime());
        CstStreamBaseResult<ObdDayTransfor> result=  cstStreamDayIntegrateService.getDayTransfor(carId, date.getTime(),HBaseTable.DAY_STATISTICS.getFirstFamilyName(),
                HbaseColumn.DayStatisticsCloumn.obdDayColumns, ObdDayTransfor.class);

        if (result != null && result.getData() != null) {
            result.getData().setFuelPerHundred(calcFuelPerHundred(result.getData().getFuel(), result.getData().getMileage()));
            result.getData().setAverageSpeed(calcAvarageSpeed(result.getData().getMileage(),result.getData().getDuration()));

        }
        return result;
    }
    //entity put obd day data
    @PutMapping(value="/save")
    @ResponseBody
    @ApiOperation(value="obd 天数据存储",httpMethod="PUT")
    public CstStreamBaseResult<ObdDayTransfor> putCstStreamObdDayTransfor(
            @ApiParam(value = "obd天数据结果", required = true) @RequestBody @NotNull ObdDayTransfor obdDayTransfor){
        log.debug("##############################obd saving day data:{}  , {}", obdDayTransfor.getCarId(), obdDayTransfor.getTime());
        CstStreamBaseResult<ObdDayTransfor> result= cstStreamDayIntegrateService.putDayTransfor(obdDayTransfor,HBaseTable.DAY_STATISTICS.getFirstFamilyName());
        if (result != null && result.getData() != null) {
            result.getData().setFuelPerHundred(calcFuelPerHundred(result.getData().getFuel(), result.getData().getMileage()));
            result.getData().setAverageSpeed(calcAvarageSpeed(result.getData().getMileage(),result.getData().getDuration()));

        }
        return result;
    }


    //通过carId 时间戳获取 obd day data
    @GetMapping(value="/nodelay/find/{carId}")
    @ResponseBody
    @ApiOperation(value="obd nodelay天数据查询",httpMethod="GET")
    public CstStreamBaseResult<ObdDayTransfor> getCstStreamObdDayTransforByTimestamp(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId){
        log.debug("########################obd get nodelay day data:{}  ",carId);
        return cstStreamDayNoDelayIntegrateService
                .getObdNoDelayDayTransfor(carId, StreamTypeDefine.OBD_TYPE,
                StreamRedisConstants.DayKey.DAY_OBD, HbaseColumn.DaySourceColumn.deDayColumns, DeHourSource.class);
    }

}

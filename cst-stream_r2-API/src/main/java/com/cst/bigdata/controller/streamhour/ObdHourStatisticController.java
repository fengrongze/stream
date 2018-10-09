package com.cst.bigdata.controller.streamhour;

import com.cst.bigdata.service.integrate.CstStreamHourIntegrateService;
import com.cst.bigdata.service.integrate.CstStreamHourNoDelayIntegrateService;
import com.cst.stream.common.HBaseTable;
import com.cst.stream.common.HbaseColumn;
import com.cst.stream.common.StreamRedisConstants;
import com.cst.stream.common.StreamTypeDefine;
import com.cst.stream.stathour.CstStreamBaseResult;
import com.cst.stream.stathour.obd.ObdHourSource;
import com.cst.stream.stathour.obd.ObdHourTransfor;
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
 * create on 2018/4/17 10:55
 * @Description Obd小时数据接口
 * @title
 */
@RestController
@RequestMapping("/stream/hour/statistics/obd")
@Api(description = "obd小时数据的查询和存储")
@Slf4j
public class ObdHourStatisticController {

    @Autowired
    private CstStreamHourIntegrateService cstStreamHourIntegrateService;

    @Autowired
    private CstStreamHourNoDelayIntegrateService cstStreamHourNoDelayIntegrateService;

    //通过carId 时间戳获取 obd hour data
    @GetMapping(value="/find/{carId}/{time}")
    @ResponseBody
    @ApiOperation(value="obd 小时数据查询",httpMethod="GET")
    public CstStreamBaseResult<ObdHourTransfor> getCstStreamObdHourTransforByTimestamp(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间戳", required = true)@PathVariable @NotNull Long time){
        log.debug("########################obd get hour data:{}  , {}",carId, time);
        CstStreamBaseResult<ObdHourTransfor> result= cstStreamHourIntegrateService.getHourTransfor(carId, time,HBaseTable.HOUR_STATISTICS.getFirstFamilyName(),
                HbaseColumn.HourStatisticsCloumn.obdHourColumns, ObdHourTransfor.class);

        if (result != null && result.getData() != null) {
            result.getData().setFuelPerHundred(calcFuelPerHundred(result.getData().getFuel(), result.getData().getMileage()));
            result.getData().setAverageSpeed(calcAvarageSpeed(result.getData().getMileage(),result.getData().getDuration()));

        }
        return result;
    }
    @GetMapping(value="/findByDateTime/{carId}/{dateTime}")
    @ResponseBody
    @ApiOperation(value="obd 小时数据查询",httpMethod="GET")
    public CstStreamBaseResult<ObdHourTransfor> getCstStreamObdHourTransforByDateTime(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间 yyyy-MM-dd HH", required = true)@PathVariable @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd HH") Date dateTime){
        log.debug("########################obd get hour data:{}  , {}",carId, dateTime.getTime());
        CstStreamBaseResult<ObdHourTransfor> result= cstStreamHourIntegrateService.getHourTransfor(carId, dateTime.getTime(),HBaseTable.HOUR_STATISTICS.getFirstFamilyName(),
                HbaseColumn.HourStatisticsCloumn.obdHourColumns, ObdHourTransfor.class);

        if (result != null && result.getData() != null) {
            result.getData().setFuelPerHundred(calcFuelPerHundred(result.getData().getFuel(), result.getData().getMileage()));
            result.getData().setAverageSpeed(calcAvarageSpeed(result.getData().getMileage(),result.getData().getDuration()));

        }
        return result;
    }
    //entity put obd hour data
    @PutMapping(value="/save")
    @ResponseBody
    @ApiOperation(value="obd 小时数据存储",httpMethod="PUT")
    public CstStreamBaseResult<ObdHourTransfor> putCstStreamGpsHourTransfor(
            @ApiParam(value = "obd 小时计算结果数据", required = true)@RequestBody @NotNull ObdHourTransfor obdHourTransfor){
        log.debug("##############################obd saving hour data:{}  , {}", obdHourTransfor.getCarId(), obdHourTransfor.getTime());
        CstStreamBaseResult<ObdHourTransfor> result= cstStreamHourIntegrateService.putHourTransfor(obdHourTransfor, HBaseTable.HOUR_STATISTICS.getFirstFamilyName());
        if (result != null && result.getData() != null) {
            result.getData().setFuelPerHundred(calcFuelPerHundred(result.getData().getFuel(), result.getData().getMileage()));
            result.getData().setAverageSpeed(calcAvarageSpeed(result.getData().getMileage(),result.getData().getDuration()));

        }
        return result;
    }


    //通过carId nodelay获取 obd hour data
    @GetMapping(value="/nodelay/find/{carId}")
    @ResponseBody
    @ApiOperation(value="obd nodelay小时数据查询",httpMethod="GET")
    public CstStreamBaseResult<ObdHourTransfor> getCstStreamObdNoDelayHourTransforByCarId(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId){
        log.debug("########################obd get hour no delay data:{} ",carId);
        CstStreamBaseResult<ObdHourTransfor> result=cstStreamHourNoDelayIntegrateService.getObdNoDelayHourTransfor(carId, StreamTypeDefine.OBD_TYPE,
                StreamRedisConstants.HourKey.HOUR_OBD, HbaseColumn.HourSourceColumn.obdHourColumns,ObdHourSource.class);

        if (result != null && result.getData() != null) {
            result.getData().setFuelPerHundred(calcFuelPerHundred(result.getData().getFuel(), result.getData().getMileage()));
            result.getData().setAverageSpeed(calcAvarageSpeed(result.getData().getMileage(),result.getData().getDuration()));
        }
        return result;
    }


}

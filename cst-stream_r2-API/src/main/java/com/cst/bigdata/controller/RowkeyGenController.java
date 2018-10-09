package com.cst.bigdata.controller;

import com.cst.bigdata.service.integrate.RowKeyGenService;
import com.cst.stream.base.BaseResult;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;

import javax.validation.constraints.NotNull;
import java.util.Date;

/**
 * @author Johnney.Chiu
 * create on 2018/4/2 10:05
 * @Description rowkeyjian
 * @title
 */
@RestController
@RequestMapping("/rowkeygen")
public class RowkeyGenController {
    Logger logger = LoggerFactory.getLogger(RowkeyGenController.class);

    @Autowired
    private RowKeyGenService rowKeyGenService;

    @ApiOperation(value = "天rowkey查询", httpMethod = "GET")
    @GetMapping(value = "/day/longtime/{carId}/{time}")
    @ResponseBody
    public BaseResult<String> getDayRowKeyByLongTime(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间戳", required = true) @PathVariable @NotNull Long time){
        logger.debug("########################Day carId and time:{}  , {}",carId, time);
        return rowKeyGenService.dayKeyGen(carId, time);
    }

    @ApiOperation(value = "天rowkey查询", httpMethod = "GET")
    @GetMapping(value = "/day/nolong/{carId}/{date}")
    @ResponseBody
    public BaseResult<String> getDayRowKeyByDateTime(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间 yyyy-MM-dd", required = true) @PathVariable @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd") Date date){
        logger.debug("########################Day carId and time:{}  , {}",carId, date.getTime());
        return rowKeyGenService.dayKeyGen(carId,  date.getTime());
    }


    @ApiOperation(value = "小时rowkey查询", httpMethod = "GET")
    @GetMapping(value = "/hour/longtime/{carId}/{time}")
    @ResponseBody
    public BaseResult<String> getHourRowKeyByLongTime(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间戳", required = true) @PathVariable @NotNull Long time){
        logger.debug("########################Hour carId and time:{}  , {}",carId, time);
        return rowKeyGenService.hourKeyGen(carId, time);
    }

    @ApiOperation(value = "小时rowkey查询", httpMethod = "GET")
    @GetMapping(value = "/hour/nolong/{carId}/{date}")
    @ResponseBody
    public BaseResult<String> getHourRowKeyByDateTime(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "时间 yyyy-MM-dd HH", required = true) @PathVariable @NotNull @DateTimeFormat(pattern = "yyyy-MM-dd HH") Date date){
        logger.debug("########################Hour carId and time:{}  , {}",carId, date.getTime());
        return rowKeyGenService.hourKeyGen(carId,  date.getTime());
    }


    @ApiOperation(value = "实时rowkey查询", httpMethod = "GET")
    @GetMapping(value = "/nodelay/{carId}/{type}")
    @ResponseBody
    public BaseResult<String> getNoDelayRowKey(
            @ApiParam(value = "车id", required = true) @PathVariable @NotNull String carId,
            @ApiParam(value = "类型数据（AM：001，de:002,gps:003,OBD：004，trace：005，treacedelete:006,voltage：007）", required = true)  @PathVariable @NotNull String type){
        logger.debug("########################rowkey no delay carId:{},{} ",carId,type);
        return rowKeyGenService.noDelayKeyGen(carId,type);
    }


}

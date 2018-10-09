package com.cst.bigdata.controller.oil;


import com.cst.bigdata.service.integrate.AppOilIntegrateService;
import com.cst.bigdata.service.integrate.RedisCommonService;
import com.cst.stream.base.BaseResult;
import com.cst.stream.vo.AppOilVo;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Nonnull;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Johnney.chiu
 * create on 2017/12/11 17:32
 * @Description 邮件
 */
@RestController
@RequestMapping("/oilPrice")
@Api(description = "城市油价查询", tags = "city oil", consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = MediaType.APPLICATION_JSON_VALUE)
public class AppOilPriceController {

    @Autowired
    private AppOilIntegrateService appOilService;

    @Autowired
    private RedisCommonService redisCommonService;

    @ApiOperation(value = "根据姓名查询用户信息", httpMethod = "GET", notes = "根据姓名查询用户信息 ")
    @RequestMapping(value = "/get/{province}", method = RequestMethod.GET)
    @ResponseBody
    public BaseResult<List<AppOilVo>> getDataFromProvince(
            @ApiParam(value = "城市", required = true) @PathVariable String province) {
        return appOilService.getDataFromProvince(province);
    }

    @ApiOperation(value = "城市油价列表-分页查询", httpMethod = "GET", notes = "城市油价列表-分页查询 ")
    @RequestMapping(value = "/get/{province}/{pageNum}/{pageSize}", method = RequestMethod.GET)
    @ResponseBody
    public BaseResult<List<AppOilVo>> getDataFromProvinceWithPage(
            @ApiParam(value = "城市", required = true) @PathVariable String province,
            @ApiParam(value = "第几页", required = true) @PathVariable Integer pageNum,
            @ApiParam(value = "每页数据", required = true) @PathVariable Integer pageSize) {
        return appOilService.getDataFromProvinceWithPage(province, pageNum, pageSize);
    }

    @ApiOperation(value = "日期查询城市油价", httpMethod = "GET", notes = "日期查询城市油价 ")
    @RequestMapping(value = "/get/{province}/{date}", method = RequestMethod.GET)
    @ResponseBody
    public BaseResult<AppOilVo> getProvinceLatestOilPrice(
            @ApiParam(value = "城市", required = true) @PathVariable String province,
            @ApiParam(value = "日期", required = true) @PathVariable @DateTimeFormat(pattern = "yyyy-MM-dd") Date date) {
        return appOilService.getProvinceLatestOilPrice(province, date);
    }


    @ApiOperation(value = "通过缓存查询油价,油标", httpMethod = "GET", notes = "通过缓存查询油价,油标 ")
    @RequestMapping(value = "/getPrice/{carId}", method = RequestMethod.GET)
    @ResponseBody
    public BaseResult<Map<String,Object>> getOilPriceWithRedisAndCarId(
            @ApiParam(value = "carid", required = true) @PathVariable @Nonnull  String carId){
        final float gasPrice = redisCommonService.calcPrice(carId, System.currentTimeMillis());
        final String gasNum = redisCommonService.getGasNumFromRedis(carId);
        Map<String, Object> map = new HashMap<>();
        map.put("gasPrice", gasPrice);
        map.put("gasNum", gasNum);
        return BaseResult.success(map);
    }
}

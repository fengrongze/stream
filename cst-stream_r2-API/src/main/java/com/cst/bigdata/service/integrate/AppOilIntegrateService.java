package com.cst.bigdata.service.integrate;

import com.cst.bigdata.service.mybatis.AppOilPriceService;
import com.cst.stream.base.BaseResult;
import com.cst.stream.base.CodeStatus;
import com.cst.stream.vo.AppOilVo;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import redis.clients.jedis.JedisCluster;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Johnney.chiu
 * create on 2017/12/12 16:10
 * @Description 整合的服务
 */
@Service
public class AppOilIntegrateService {

    @Autowired
    private AppOilPriceService appOilPriceService;

    @Autowired
    private JedisCluster jedisCluster;

    public BaseResult<List<AppOilVo>> getDataFromProvinceWithPage(String province, Integer pageNum,
                                                                  Integer pageSize) {

        List<AppOilVo> appOilVos = appOilPriceService.getAppOilPriceWithProvincePage(province, pageNum, pageSize)
                .stream()
                .map(a -> {
                    AppOilVo appOilVo = new AppOilVo();
                    BeanUtils.copyProperties(a, appOilVo);
                    return appOilVo;
                }).collect(Collectors.toList());
        return BaseResult.<List<AppOilVo>>builder().data(appOilVos).success(true).code(CodeStatus.SUCCESS_CODE).description("success")
                .build();
    }

    public BaseResult<List<AppOilVo>> getDataFromProvince( String province) {
        List<AppOilVo> appOilVos = appOilPriceService.getAppOilPriceWithProvince(province)
                .stream()
                .map(a -> {
                    AppOilVo appOilVo = new AppOilVo();
                    BeanUtils.copyProperties(a, appOilVo);
                    return appOilVo;
                }).collect(Collectors.toList());
        return BaseResult.success(appOilVos);
    }

    public BaseResult<AppOilVo> getProvinceLatestOilPrice(String province,
                                              Date date) {
        AppOilVo appOilVo = new AppOilVo();
        BeanUtils.copyProperties(appOilPriceService.getProvinceLatestOilPrice(date, province),appOilVo);
        return BaseResult.success(appOilVo);
    }




}

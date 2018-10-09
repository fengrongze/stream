package com.cst.bigdata.service.mybatis;

import com.cst.bigdata.domain.mybatis.AppOilPrice;

import java.util.Date;
import java.util.List;

/**
 * @author Johnney.chiu
 * create on 2017/12/11 16:56
 * @Description get oildata
 */
public interface AppOilPriceService {

    AppOilPrice getProvinceLatestOilPrice(Date insertTimestamp, String province);

    List<AppOilPrice> getAppOilPriceWithProvince(String province);

    List<AppOilPrice> getAppOilPriceWithProvincePage(String province,Integer pageNum, Integer pageSize);
}

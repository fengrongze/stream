package com.cst.bigdata.service.mybatis.impl;

import com.cst.bigdata.domain.mybatis.AppOilPrice;
import com.cst.bigdata.domain.mybatis.AppOilPriceExample;
import com.cst.bigdata.repository.mybatis.AppOilPriceMapper;
import com.cst.bigdata.service.mybatis.AppOilPriceService;
import com.github.pagehelper.Page;
import com.github.pagehelper.PageHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.List;

/**
 * @author Johnney.chiu
 * create on 2017/12/11 17:06
 * @Description shixianlei
 */
@Service
public class AppOilPriceServiceImpl implements AppOilPriceService {

    @Autowired
    private AppOilPriceMapper appOilPriceMapper;

    @Override
    public AppOilPrice getProvinceLatestOilPrice(Date insertTimestamp, String province) {
        AppOilPriceExample appOilPriceExample = new AppOilPriceExample();
        appOilPriceExample.setOrderByClause("insert_timestamp desc");
        appOilPriceExample.createCriteria().andInsertTimestampLessThan(insertTimestamp).
                andProvinceEqualTo(province);
        return appOilPriceMapper.selectByExample(appOilPriceExample).get(0);
    }

    @Override
    public List<AppOilPrice> getAppOilPriceWithProvince(String province) {
        AppOilPriceExample appOilPriceExample = new AppOilPriceExample();
        appOilPriceExample.createCriteria().andProvinceEqualTo(province);
        return appOilPriceMapper.selectByExample(appOilPriceExample);
    }
    @Override
    public List<AppOilPrice> getAppOilPriceWithProvincePage(String province,Integer pageNum, Integer pageSize) {
        AppOilPriceExample appOilPriceExample = new AppOilPriceExample();
        appOilPriceExample.createCriteria().andProvinceEqualTo(province);
        PageHelper.startPage(pageNum, pageSize);
        Page<AppOilPrice> list = (Page<AppOilPrice>) appOilPriceMapper.selectByExample(appOilPriceExample);
        return list;
    }
}

package com.cst.bigdata.repository.mybatis;

import com.cst.bigdata.domain.mybatis.AppOilPrice;
import com.cst.bigdata.domain.mybatis.AppOilPriceExample;
import java.util.List;
import org.apache.ibatis.annotations.Param;

public interface AppOilPriceMapper {
    int countByExample(AppOilPriceExample example);

    int deleteByExample(AppOilPriceExample example);

    int deleteByPrimaryKey(String id);

    int insert(AppOilPrice record);

    int insertSelective(AppOilPrice record);

    List<AppOilPrice> selectByExample(AppOilPriceExample example);

    AppOilPrice selectByPrimaryKey(String id);

    int updateByExampleSelective(@Param("record") AppOilPrice record, @Param("example") AppOilPriceExample example);

    int updateByExample(@Param("record") AppOilPrice record, @Param("example") AppOilPriceExample example);

    int updateByPrimaryKeySelective(AppOilPrice record);

    int updateByPrimaryKey(AppOilPrice record);
}
package com.cst.bigdata.service.integrate;

import com.cst.bigdata.service.hbase.CstStreamHourStatisticsService;
import com.cst.stream.base.BaseResult;
import com.cst.stream.base.CodeStatus;
import com.cst.stream.common.CstConstants;
import com.cst.stream.common.DateTimeUtils;
import com.cst.stream.common.RowKeyGenerate;
import com.cst.stream.stathour.CSTData;
import com.cst.stream.stathour.CstStreamBaseResult;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import redis.clients.jedis.JedisCluster;

import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Johnney.chiu
 * create on 2017/12/21 15:17
 * @Description 整合的天数据整理
 */
@Service
public class CstStreamHourIntegrateService<T extends CSTData> {

    Logger logger = LoggerFactory.getLogger(CstStreamHourIntegrateService.class);

    @Autowired
    private CstStreamHourStatisticsService<T> cstStreamHourStatisticsService;



    public CstStreamBaseResult<T> getHourTransfor(String carId, Long time,String familyName,String[] columns,Class<?> clazz){
        String rowKey = null;
        try {
            rowKey = RowKeyGenerate.getRowKeyById(carId, time, CstConstants.TIME_SELECT.HOUR);
            T data = cstStreamHourStatisticsService.getHourTransforDataByRowKey(familyName,rowKey,columns,clazz);
            return CstStreamBaseResult.success(data);
        } catch (ParseException e) {
            logger.info("create row key error：{}",e);
        }
        return CstStreamBaseResult.fail(CodeStatus.GENERAL_ERROR_CODE, "error create rowkey");
    }


    public CstStreamBaseResult<T> getHourTransfor(String carId, Long time, Map familyQulifiers, String[] columns, Class<?> clazz){
        String rowKey = null;
        try {
            rowKey = RowKeyGenerate.getRowKeyById(carId, time, CstConstants.TIME_SELECT.HOUR);
            T data = cstStreamHourStatisticsService.getHourTransforDataByRowKey(familyQulifiers,rowKey,columns,clazz);
            return CstStreamBaseResult.success(data);
        } catch (ParseException e) {
            logger.info("create row key error：{}",e);
        }
        return CstStreamBaseResult.fail(CodeStatus.GENERAL_ERROR_CODE, "error create rowkey");
    }


    public CstStreamBaseResult<T> putHourTransfor(T data,String familyName){
        String rowKey = null;
        try {
            rowKey = RowKeyGenerate.getRowKeyById(data.getCarId(),data.getTime(),
                    CstConstants.TIME_SELECT.HOUR);
            cstStreamHourStatisticsService.putHourTransforData(data,familyName,rowKey);
            return CstStreamBaseResult.success();
        } catch (ParseException e) {
            logger.info("create row key error：{}",e);
        }
        return CstStreamBaseResult.fail(CodeStatus.GENERAL_ERROR_CODE, "error create rowkey");
    }


    public BaseResult<List<T>> getHourTransforBetween(String carId, Long fromTime, Long toTime, Map<String, String[]> familyMap, String[] columns, Class<?> clazz) {
        List<Long> listArea;
        if(fromTime>=toTime) {
            listArea = DateTimeUtils.getBetweenTimestamp(toTime, fromTime, CstConstants.TIME_SELECT.HOUR);
        }else{
            listArea = DateTimeUtils.getBetweenTimestamp(fromTime,toTime, CstConstants.TIME_SELECT.HOUR);
        }

        listArea.add(fromTime);
        listArea.add(toTime);
        List<String> rowKeys=listArea.parallelStream().map(l -> {
            try {
                return RowKeyGenerate.getRowKeyById(carId, l, CstConstants.TIME_SELECT.HOUR);
            } catch (ParseException e) {
                logger.error("parse hour change error", e);
            }
            return null;
        }).filter(str-> StringUtils.isNotBlank(str)).distinct().collect(Collectors.toList());
        List<T> data = cstStreamHourStatisticsService.getHourTransforDataBetween(familyMap,rowKeys,clazz);
        return BaseResult.success(data);
    }
}

package com.cst.bigdata.service.integrate;

import com.cst.bigdata.service.hbase.CstStreamDayStatisticsService;
import com.cst.stream.base.BaseResult;
import com.cst.stream.base.CodeStatus;
import com.cst.stream.common.*;
import com.cst.stream.stathour.CSTData;
import com.cst.stream.stathour.CstStreamBaseResult;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Johnney.chiu
 * create on 2017/12/21 15:17
 * @Description 整合的天数据整理
 */
@Service
public class CstStreamDayIntegrateService<T extends CSTData> {
    Logger logger = LoggerFactory.getLogger(CstStreamDayIntegrateService.class);

    @Autowired
    private CstStreamDayStatisticsService<T> cstStreamDayStatisticsService;

    public CstStreamBaseResult<T> getDayTransfor(String carId, Long time,String familyName,String[] columns,Class<?> clazz){
        String rowKey = null;
        try {
            rowKey = RowKeyGenerate.getRowKeyById(carId, time, CstConstants.TIME_SELECT.DAY);
            T data = cstStreamDayStatisticsService.getDayTransforDataByRowKey(familyName,rowKey,columns,clazz);
            return CstStreamBaseResult.success(data);
        } catch (ParseException e) {
            logger.info("create row key error：{}",e);
        }
        return CstStreamBaseResult.fail(CodeStatus.GENERAL_ERROR_CODE, "error create rowkey");

    }

    public CstStreamBaseResult<T> putDayTransfor(T data,String familyName){
        String rowKey = null;
        try {
            rowKey = RowKeyGenerate.getRowKeyById(data.getCarId(),data.getTime(),
                    CstConstants.TIME_SELECT.DAY);
            cstStreamDayStatisticsService.putDayTransforData(data,familyName,rowKey);
            return CstStreamBaseResult.success();
        } catch (ParseException e) {
            logger.info("create row key error：{}",e);
        }
        return CstStreamBaseResult.fail(CodeStatus.GENERAL_ERROR_CODE, "error create rowkey");
    }
    public CstStreamBaseResult<T> getDayTransfor(String carId, Long time, Map familyQulifiers, String[] columns, Class<?> clazz){
        String rowKey = null;
        try {
            rowKey = RowKeyGenerate.getRowKeyById(carId, time, CstConstants.TIME_SELECT.DAY);
            T data = cstStreamDayStatisticsService.getDayTransforDataByRowKey(familyQulifiers,rowKey,columns,clazz);
            return CstStreamBaseResult.success(data);
        } catch (ParseException e) {
            logger.info("create row key error：{}",e);
        }
        return CstStreamBaseResult.fail(CodeStatus.GENERAL_ERROR_CODE, "error create rowkey");

    }

    public BaseResult<Map<String,T>> getDayTransforCarIds(String[] carIds, Long time, Map familyQulifiers, String[] columns, Class<?> clazz){
        try {
            Map<String, T> datas = Arrays.asList(carIds).parallelStream().distinct().map(carId -> {
                T data = null;
                try {
                    String rowKey = RowKeyGenerate.getRowKeyById(carId, time, CstConstants.TIME_SELECT.DAY);
                    data = cstStreamDayStatisticsService.getDayTransforDataByRowKey(familyQulifiers, rowKey, columns, clazz);
                } catch (ParseException e) {
                    logger.error("parse exception ", e);
                }
                return data;
            }).filter(t -> t != null && StringUtils.isNotBlank(t.getCarId())).collect(Collectors.toMap(T::getCarId, t -> t));
            return BaseResult.success(datas);
        }catch (Throwable e){
            return BaseResult.fail(CodeStatus.GENERAL_EXCEPTION_CODE,"data get error");
        }

    }

    public BaseResult<List<T>> getDayTransforBetween(String carId, Long fromTime, Long toTime,
                                                                         Map<String, String[]> familyMap, String[] allColumns, Class<?> clazz) {
        List<Long> listArea;
        if(fromTime>=toTime) {
            listArea = DateTimeUtils.getBetweenTimestamp(toTime, fromTime, CstConstants.TIME_SELECT.DAY);
        }else{
            listArea = DateTimeUtils.getBetweenTimestamp(fromTime,toTime, CstConstants.TIME_SELECT.DAY);
        }

        listArea.add(fromTime);
        listArea.add(toTime);
        List<String> rowKeys=listArea.parallelStream().map(l -> {
            try {
                return RowKeyGenerate.getRowKeyById(carId, l, CstConstants.TIME_SELECT.DAY);
            } catch (ParseException e) {
                logger.error("parse day change error", e);
            }
            return null;
        }).filter(str->StringUtils.isNotBlank(str)).distinct().collect(Collectors.toList());
        List<T> data = cstStreamDayStatisticsService.getDayTransforDataBetween(familyMap,rowKeys,clazz);
        return BaseResult.success(data);

    }
    public BaseResult<Map<String,List<T>>> getDayTransforBetweenByCarIds(String[] carIds, Long fromTime, Long toTime,
                                                                         Map<String, String[]> familyMap, String[] allColumns, Class<?> clazz) {
       Map<String,List<T>> result =Arrays.asList(carIds).parallelStream().distinct().map(carId->{
            List<T> datas;
           List<Long> listArea;
           if(fromTime>=toTime) {
               listArea = DateTimeUtils.getBetweenTimestamp(toTime, fromTime, CstConstants.TIME_SELECT.DAY);
           }else{
               listArea = DateTimeUtils.getBetweenTimestamp(fromTime,toTime, CstConstants.TIME_SELECT.DAY);
           }
           listArea.add(fromTime);
           listArea.add(toTime);
           List<String> rowKeys=listArea.parallelStream().map(l -> {
               try {
                   return RowKeyGenerate.getRowKeyById(carId, l, CstConstants.TIME_SELECT.DAY);
               } catch (ParseException e) {
                   logger.error("parse day change error", e);

               }
               return null;
           }).filter(str->StringUtils.isNotBlank(str)).distinct().collect(Collectors.toList());
           datas = cstStreamDayStatisticsService.getDayTransforDataBetween(familyMap,rowKeys,clazz);
            return datas;
        }).flatMap(l->l.stream().filter(t->t!=null)).collect(Collectors.groupingBy(T::getCarId));

        return BaseResult.success(result);



    }
}

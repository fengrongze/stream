package com.cst.bigdata.service.integrate;

import com.cst.bigdata.service.hbase.CstStreamYearStatisticsService;
import com.cst.stream.base.BaseResult;
import com.cst.stream.base.CodeStatus;
import com.cst.stream.common.CstConstants;
import com.cst.stream.common.DateTimeUtils;
import com.cst.stream.common.RowKeyGenerate;
import com.cst.stream.stathour.CSTData;
import com.cst.stream.stathour.CstStreamBaseResult;
import com.cst.stream.stathour.integrated.YearIntegratedTransfor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.ParseException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Johnney.chiu
 * create on 2017/12/21 15:17
 * @Description 整合的年数据整理
 */
@Service
@Slf4j
public class CstStreamYearIntegrateService<T extends CSTData> {

    @Autowired
    private CstStreamYearStatisticsService<T> cstStreamYearStatisticsService;

    public CstStreamBaseResult<T> getYearTransfor(String carId, Long time,String familyName,String[] columns,Class<?> clazz){
        String rowKey = null;
        try {
            rowKey = RowKeyGenerate.getRowKeyById(carId, time, CstConstants.TIME_SELECT.YEAR);

            T data = cstStreamYearStatisticsService.getYearTransforDataByRowKey(familyName,rowKey,columns,clazz);
            return CstStreamBaseResult.success(data);
        } catch (ParseException e) {
            log.info("create row key error：{}",e);
        }
        return CstStreamBaseResult.fail(CodeStatus.GENERAL_ERROR_CODE, "error create rowkey");

    }

    public BaseResult<Map<String,T>> getYearTransforCarIds(String[] carIds, Long time, Map familyQulifiers, String[] columns, Class<?> clazz){
        Map<String,T> datas=Arrays.asList(carIds).stream().map(carId->{
                T data = null;
                try {
                    String rowKey = RowKeyGenerate.getRowKeyById(carId, time, CstConstants.TIME_SELECT.YEAR);
                    data=cstStreamYearStatisticsService.getYearTransforDataByRowKey(familyQulifiers,rowKey,columns,clazz);
                } catch (ParseException e) {
                    log.error("parse exception ",e);
                }
                return data;
            }).filter(t->t!=null).collect(Collectors.toMap(T::getCarId,t->t));
            return BaseResult.success(datas);
    }

    public CstStreamBaseResult<T> putYearTransfor(T data,String familyName){
        String rowKey = null;
        try {
            rowKey = RowKeyGenerate.getRowKeyById(data.getCarId(),data.getTime(),
                    CstConstants.TIME_SELECT.YEAR);
            cstStreamYearStatisticsService.putYearTransforData(data,familyName,rowKey);
            return CstStreamBaseResult.success();
        } catch (ParseException e) {
            log.info("create row key error：{}",e);
        }
        return CstStreamBaseResult.fail(CodeStatus.GENERAL_ERROR_CODE, "error create rowkey");
    }
    public CstStreamBaseResult<T> getYearTransfor(String carId, Long time, Map familyQulifiers, String[] columns, Class<?> clazz){
        String rowKey = null;
        try {
            rowKey = RowKeyGenerate.getRowKeyById(carId, time, CstConstants.TIME_SELECT.YEAR);
            T data = cstStreamYearStatisticsService.getYearTransforDataByRowKey(familyQulifiers,rowKey,columns,clazz);
            return CstStreamBaseResult.success(data);
        } catch (ParseException e) {
            log.info("create row key error：{}",e);
        }
        return CstStreamBaseResult.fail(CodeStatus.GENERAL_ERROR_CODE, "error create rowkey");

    }

    public BaseResult<List<YearIntegratedTransfor>> getYearTransforBetween(String carId, Long fromTime, Long toTime,
                                                                         Map<String, String[]> familyMap, String[] allColumns, Class<?> clazz
    ) {
        List<Long> listArea;
        if(fromTime>=toTime) {
            listArea = DateTimeUtils.getBetweenTimestamp(toTime, fromTime, CstConstants.TIME_SELECT.YEAR);
        }else{
            listArea = DateTimeUtils.getBetweenTimestamp(fromTime,toTime, CstConstants.TIME_SELECT.YEAR);
        }

        listArea.add(fromTime);
        listArea.add(toTime);
        List<String> rowKeys=listArea.parallelStream().map(l -> {
            try {
                return RowKeyGenerate.getRowKeyById(carId, l, CstConstants.TIME_SELECT.YEAR);
            } catch (ParseException e) {
                log.error("parse year change error", e);
            }
            return null;
        }).filter(str-> StringUtils.isNotBlank(str)).distinct().collect(Collectors.toList());
        List<T> data = cstStreamYearStatisticsService.getYearTransforDataBetween(familyMap,rowKeys,clazz);
        return BaseResult.success(data);

    }
}

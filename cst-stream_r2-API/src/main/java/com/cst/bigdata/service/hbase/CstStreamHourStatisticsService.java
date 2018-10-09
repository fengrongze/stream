package com.cst.bigdata.service.hbase;

import com.cst.bigdata.repository.hbase.CstStreamHourStatisticsMapper;
import com.cst.stream.common.HBaseTable;
import com.cst.stream.stathour.CSTData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * @author Johnney.chiu
 * create on 2017/12/19 18:09
 * @Description 小时统计服务
 */

@Service
public class CstStreamHourStatisticsService<T extends CSTData> {

    @Autowired
    private CstStreamHourStatisticsMapper<T> cstStreamHourStatisticsMapper;


    public T getHourTransforDataByRowKey(String familyName,String rowKey,String[] columns,Class<?> clazz){
        return cstStreamHourStatisticsMapper.findHourTransforByRowKey(HBaseTable.HOUR_STATISTICS.getTableName(),
                familyName,rowKey,columns,clazz);
    }


    public T getHourTransforDataByRowKey(Map familyQulifiers, String rowKey, String[] columns, Class<?> clazz){
        return cstStreamHourStatisticsMapper.findHourTransforByRowKey(HBaseTable.HOUR_STATISTICS.getTableName(),familyQulifiers,
                rowKey,columns,clazz);
    }
    public void putHourTransforData(T data,String familyName,String rowKey){
        cstStreamHourStatisticsMapper.putHourTransfor(HBaseTable.HOUR_STATISTICS.getTableName(),
                familyName,rowKey,data);
    }

    public List<T> getHourTransforDataBetween(Map<String, String[]> familyMap, String fromRowKey, String toRowKey, String[] columns, Class<?> clazz) {
        return cstStreamHourStatisticsMapper.findHourTransforByScan(HBaseTable.HOUR_STATISTICS.getTableName(),familyMap,fromRowKey,toRowKey
                ,columns,clazz);
    }

    public List<T> getHourTransforDataBetween(Map<String, String[]> familyMap,List<String> rowKeys, Class<?> clazz) {
        return cstStreamHourStatisticsMapper.findHourTransforByRowKeys(HBaseTable.HOUR_STATISTICS.getTableName(),familyMap,rowKeys
                , clazz);
    }
}

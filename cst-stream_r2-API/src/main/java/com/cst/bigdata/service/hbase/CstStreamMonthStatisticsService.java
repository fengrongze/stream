package com.cst.bigdata.service.hbase;

import com.cst.bigdata.repository.hbase.CstStreamMonthStatisticsMapper;
import com.cst.stream.common.HBaseTable;
import com.cst.stream.stathour.CSTData;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * @author Johnney.chiu
 * create on 2017/12/19 18:09
 * @Description 月统计服务
 */

@Service
@Slf4j
public class CstStreamMonthStatisticsService<T extends CSTData> {

    @Autowired
    private CstStreamMonthStatisticsMapper<T> cstStreamMonthStatisticsMapper;

    public T getMonthTransforDataByRowKey(String familyName,String rowKey,String columns[],Class<?> clazz){
        log.info("table {} familiy {} rowkey {},columns",HBaseTable.MONTH_STATISTICS.getTableName(),familyName,rowKey,columns);
        return cstStreamMonthStatisticsMapper.findMonthTransforByRowKey(HBaseTable.MONTH_STATISTICS.getTableName(),
                familyName,rowKey,columns,clazz);
    }
    public T getMonthTransforDataByRowKey(Map familyQulifiers, String rowKey, String columns[], Class<?> clazz){
        return cstStreamMonthStatisticsMapper.findMonthTransforByRowKey(HBaseTable.MONTH_STATISTICS.getTableName(),
                familyQulifiers,rowKey,columns,clazz);
    }
    public void putMonthTransforData(T data,String familyName,String rowKey){
        cstStreamMonthStatisticsMapper.putMonthTransfor(HBaseTable.MONTH_STATISTICS.getTableName(),
                familyName,rowKey,data);
    }

    public <T extends CSTData> List<T> getMonthTransforDataBetween(Map<String, String[]> familyMap, String fromRowKey, String toRowKey, String[] allColumns, Class<?> clazz) {
        return cstStreamMonthStatisticsMapper.findMonthTransforByScan(HBaseTable.MONTH_STATISTICS.getTableName(),familyMap,fromRowKey,toRowKey
                ,allColumns,clazz);
    }

    public <T extends CSTData> List<T> getMonthTransforDataBetween(Map<String, String[]> familyMap, List<String> rowKeys, Class<?> clazz) {
        return cstStreamMonthStatisticsMapper.findMonthTransforByRowKeys(HBaseTable.MONTH_STATISTICS.getTableName(),familyMap,rowKeys
                ,clazz);
    }
}

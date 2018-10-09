package com.cst.bigdata.service.hbase;

import com.cst.bigdata.repository.hbase.CstStreamYearStatisticsMapper;
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
 * @Description 年统计服务
 */

@Service
@Slf4j
public class CstStreamYearStatisticsService<T extends CSTData> {

    @Autowired
    private CstStreamYearStatisticsMapper<T> cstStreamYearStatisticsMapper;

    public T getYearTransforDataByRowKey(String familyName,String rowKey,String columns[],Class<?> clazz){
        log.info("table {} familiy {} rowkey {},columns",HBaseTable.YEAR_STATISTICS.getTableName(),familyName,rowKey,columns);
        return cstStreamYearStatisticsMapper.findYearTransforByRowKey(HBaseTable.YEAR_STATISTICS.getTableName(),
                familyName,rowKey,columns,clazz);
    }
    public T getYearTransforDataByRowKey(Map familyQulifiers, String rowKey, String columns[], Class<?> clazz){
        return cstStreamYearStatisticsMapper.findYearTransforByRowKey(HBaseTable.YEAR_STATISTICS.getTableName(),
                familyQulifiers,rowKey,columns,clazz);
    }
    public void putYearTransforData(T data,String familyName,String rowKey){
        cstStreamYearStatisticsMapper.putYearTransfor(HBaseTable.YEAR_STATISTICS.getTableName(),
                familyName,rowKey,data);
    }

    public <T extends CSTData> List<T> getYearTransforDataBetween(Map<String, String[]> familyMap, String fromRowKey, String toRowKey, String[] allColumns, Class<?> clazz) {
        return cstStreamYearStatisticsMapper.findYearTransforByScan(HBaseTable.YEAR_STATISTICS.getTableName(),familyMap,fromRowKey,toRowKey
                ,allColumns,clazz);
    }

    public <T extends CSTData> List<T> getYearTransforDataBetween(Map<String, String[]> familyMap, List<String> rowKeys, Class<?> clazz) {
        return cstStreamYearStatisticsMapper.findYearTransforByRowKeys(HBaseTable.YEAR_STATISTICS.getTableName(),familyMap,rowKeys
                ,clazz);
    }
}

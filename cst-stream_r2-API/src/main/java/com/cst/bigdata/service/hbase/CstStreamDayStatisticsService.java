package com.cst.bigdata.service.hbase;

import com.cst.bigdata.repository.hbase.CstStreamDayStatisticsMapper;
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
 * @Description 小时统计服务
 */

@Service
@Slf4j
public class CstStreamDayStatisticsService<T extends CSTData> {

    @Autowired
    private CstStreamDayStatisticsMapper<T> cstStreamDayStatisticsMapper;

    public T getDayTransforDataByRowKey(String familyName,String rowKey,String columns[],Class<?> clazz){
        log.info("table {} familiy {} rowkey {},columns",HBaseTable.DAY_STATISTICS.getTableName(),familyName,rowKey,columns);
        return cstStreamDayStatisticsMapper.findDayTransforByRowKey(HBaseTable.DAY_STATISTICS.getTableName(),
                familyName,rowKey,columns,clazz);
    }
    public T getDayTransforDataByRowKey(Map familyQulifiers, String rowKey, String columns[], Class<?> clazz){
        return cstStreamDayStatisticsMapper.findDayTransforByRowKey(HBaseTable.DAY_STATISTICS.getTableName(),
                familyQulifiers,rowKey,columns,clazz);
    }
    public void putDayTransforData(T data,String familyName,String rowKey){
        cstStreamDayStatisticsMapper.putDayTransfor(HBaseTable.DAY_STATISTICS.getTableName(),
                familyName,rowKey,data);
    }

    public <T extends CSTData> List<T> getDayTransforDataBetween(Map<String, String[]> familyMap, List<String> rowKeys, Class<?> clazz) {
        return cstStreamDayStatisticsMapper.findDayTransforByRowKeys(HBaseTable.DAY_STATISTICS.getTableName(),familyMap,rowKeys
                ,clazz);
    }
}

package com.cst.bigdata.service.hbase;

import com.cst.bigdata.repository.hbase.CstStreamHourSourceDataMapper;
import com.cst.stream.common.HBaseTable;
import com.cst.stream.stathour.CSTData;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author Johnney.Chiu
 * create on 2018/6/8 16:29
 * @Description 查询小时第一条数据
 * @title
 */
@Service
@Slf4j
public class CstStreamHourSourceService<S extends CSTData> {


    @Autowired
    private CstStreamHourSourceDataMapper<S> cstStreamHourStatisticsMapper;

    public S getHourSourceDataByRowKey(String rowKey,String columns[],Class<?> clazz){
        log.info("get first data from table {},{},{},{},{}",HBaseTable.HOUR_FIRST_ZONE.getTableName(),
                HBaseTable.HOUR_FIRST_ZONE.getFirstFamilyName(),rowKey,columns,clazz);
        return cstStreamHourStatisticsMapper.findSourceDataByRowKey(HBaseTable.HOUR_FIRST_ZONE.getTableName(),
                HBaseTable.HOUR_FIRST_ZONE.getFirstFamilyName(),rowKey,columns,clazz);
    }
    public void putHourSourceData(S sourceData,String rowKey){
        cstStreamHourStatisticsMapper.putSourceData(HBaseTable.HOUR_STATISTICS.getTableName(),
                HBaseTable.HOUR_STATISTICS.getFirstFamilyName(),rowKey,sourceData);
    }
}

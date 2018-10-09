package com.cst.bigdata.service.hbase;

import com.cst.bigdata.repository.hbase.CstStreamDaySourceDataMapper;
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
public class CstStreamDaySourceService<S extends CSTData> {


    @Autowired
    private CstStreamDaySourceDataMapper<S> cstStreamDayStatisticsMapper;

    public S getDaySourceDataByRowKey(String rowKey, String columns[], Class<?> clazz){
        log.info("get first data from table {},{},{},{},{}",HBaseTable.DAY_FIRST_ZONE.getTableName(),
                HBaseTable.DAY_FIRST_ZONE.getFirstFamilyName(),rowKey,columns,clazz);
        return cstStreamDayStatisticsMapper.findSourceDataByRowKey(HBaseTable.DAY_FIRST_ZONE.getTableName(),
                HBaseTable.DAY_FIRST_ZONE.getFirstFamilyName(),rowKey,columns,clazz);
    }
    public void putDaySourceData(S sourceData, String rowKey){
        cstStreamDayStatisticsMapper.putSourceData(HBaseTable.DAY_FIRST_ZONE.getTableName(),
                HBaseTable.DAY_FIRST_ZONE.getFirstFamilyName(),rowKey,sourceData);
    }
}

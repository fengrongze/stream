package com.cst.bigdata.service.integrate;

import com.cst.bigdata.service.hbase.CstStreamHourSourceService;
import com.cst.stream.base.CodeStatus;
import com.cst.stream.common.CstConstants;
import com.cst.stream.common.RowKeyGenerate;
import com.cst.stream.stathour.CSTData;
import com.cst.stream.stathour.CstStreamBaseResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.ParseException;

/**
 * @author Johnney.Chiu
 * create on 2018/6/8 16:25
 * @Description
 * @title
 */
@Slf4j
@Service
public class CstStreamHourFirstIntegrateService<S extends CSTData> {


    @Autowired
    private CstStreamHourSourceService<S> cstStreamHourSourceService;

    public CstStreamBaseResult<S> getHourSource(String carId, Long time,String type,String[] columns,Class<?> clazz){
        String rowKey = null;
        try {
            rowKey = RowKeyGenerate.getRowKeyById(carId, time, CstConstants.TIME_SELECT.HOUR,type);
            S amSourceData = cstStreamHourSourceService.getHourSourceDataByRowKey(rowKey,columns,clazz);
            return CstStreamBaseResult.success(amSourceData);
        } catch (ParseException e) {
            log.info("create row key error：{}",e);
        }
        return CstStreamBaseResult.fail(CodeStatus.GENERAL_ERROR_CODE, "error create rowkey");
    }

    public CstStreamBaseResult<S> putHourSource(S s,String type){
        String rowKey = null;
        try {
            rowKey = RowKeyGenerate.getRowKeyById(s.getCarId(),s.getTime(),
                    CstConstants.TIME_SELECT.HOUR,type);
            cstStreamHourSourceService.putHourSourceData(s,rowKey);
            return CstStreamBaseResult.success();
        } catch (ParseException e) {
            log.info("create row key error：{}",e);
        }
        return CstStreamBaseResult.fail(CodeStatus.GENERAL_ERROR_CODE, "error create rowkey");
    }
}

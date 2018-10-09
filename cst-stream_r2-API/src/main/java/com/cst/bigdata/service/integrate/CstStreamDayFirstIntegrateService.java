package com.cst.bigdata.service.integrate;

import com.cst.bigdata.service.hbase.CstStreamDaySourceService;
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
public class CstStreamDayFirstIntegrateService<S extends CSTData> {


    @Autowired
    private CstStreamDaySourceService<S> cstStreamDaySourceService;

    public CstStreamBaseResult<S> getDaySource(String carId, Long time, String type, String[] columns, Class<?> clazz){
        String rowKey = null;
        try {
            rowKey = RowKeyGenerate.getRowKeyById(carId, time, CstConstants.TIME_SELECT.DAY,type);
            S amSourceData = cstStreamDaySourceService.getDaySourceDataByRowKey(rowKey,columns,clazz);
            return CstStreamBaseResult.success(amSourceData);
        } catch (ParseException e) {
            log.info("create row key error：{}",e);
        }
        return CstStreamBaseResult.fail(CodeStatus.GENERAL_ERROR_CODE, "error create rowkey");
    }

    public CstStreamBaseResult<S> putDaySource(S s, String type){
        String rowKey = null;
        try {
            rowKey = RowKeyGenerate.getRowKeyById(s.getCarId(),s.getTime(),
                    CstConstants.TIME_SELECT.DAY,type);
            cstStreamDaySourceService.putDaySourceData(s,rowKey);
            return CstStreamBaseResult.success();
        } catch (ParseException e) {
            log.info("create row key error：{}",e);
        }
        return CstStreamBaseResult.fail(CodeStatus.GENERAL_ERROR_CODE, "error create rowkey");
    }
}

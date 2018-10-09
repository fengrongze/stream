package com.cst.bigdata.service.integrate;

import com.cst.stream.base.BaseResult;
import com.cst.stream.base.CodeStatus;
import com.cst.stream.common.CstConstants;
import com.cst.stream.common.RowKeyGenerate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.text.ParseException;

/**
 * @author Johnney.Chiu
 * create on 2018/4/2 10:08
 * @Description 11
 * @title
 */
@Service
public class RowKeyGenService {

    private static final Logger logger = LoggerFactory.getLogger(RowKeyGenService.class);

    public BaseResult<String> dayKeyGen(String carId, Long time){
        String rowKey = null;
        try {
            rowKey = RowKeyGenerate.getRowKeyById(carId, time, CstConstants.TIME_SELECT.DAY);
            return BaseResult.success(rowKey);
        } catch (ParseException e) {
            logger.info("create row key error：{}",e);
        }
        return BaseResult.fail(CodeStatus.GENERAL_ERROR_CODE, "error create rowkey");

    }

    public BaseResult<String> hourKeyGen(String carId,Long time){
        String rowKey = null;
        try {
            rowKey = RowKeyGenerate.getRowKeyById(carId, time, CstConstants.TIME_SELECT.HOUR);
            return BaseResult.success(rowKey);
        } catch (ParseException e) {
            logger.info("create row key error：{}",e);
        }
        return BaseResult.fail(CodeStatus.GENERAL_ERROR_CODE, "error create rowkey");

    }

    public BaseResult<String> noDelayKeyGen(String carId){
            String rowKey = RowKeyGenerate.getRowKeyById(carId);
            return BaseResult.success(rowKey);
    }
    public BaseResult<String> noDelayKeyGen(String carId,String type){
        String rowKey = RowKeyGenerate.getRowKeyById(carId,type);
        return BaseResult.success(rowKey);
    }

}

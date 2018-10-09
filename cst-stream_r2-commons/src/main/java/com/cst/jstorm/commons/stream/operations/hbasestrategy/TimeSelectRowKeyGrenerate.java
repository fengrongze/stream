package com.cst.jstorm.commons.stream.operations.hbasestrategy;

import com.cst.stream.common.CstConstants;
import com.cst.stream.common.RowKeyGenerate;
import com.cst.stream.stathour.CSTData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;

/**
 * @author Johnney.Chiu
 * create on 2018/3/1 11:13
 * @Description 由时间与key生成的rowkey
 * @title
 */
public class TimeSelectRowKeyGrenerate<T extends CSTData> implements IRowKeyGrenate<T>{
    Logger logger = LoggerFactory.getLogger(TimeSelectRowKeyGrenerate.class);
    private String carId;

    private Long time;

    private CstConstants.TIME_SELECT time_select;

    private T t;

    public TimeSelectRowKeyGrenerate(String carId, Long time, CstConstants.TIME_SELECT time_select) {
        this.carId = carId;
        this.time = time;
        this.time_select = time_select;
    }

    public TimeSelectRowKeyGrenerate(CstConstants.TIME_SELECT time_select, T t) {
        this.time_select = time_select;
        this.t = t;
    }

    @Override
    public String createrowkey() {
        return generateRowKey();
    }

    public String generateRowKey() {
        try {
            if (t != null)
                return generateRowKey(t.getCarId(), t.getTime(), time_select);
            else
                return generateRowKey(carId, time, time_select);

            } catch (ParseException e) {
                logger.error("generate row key error:{},{},{}",carId,time,e);
            }
        return "";
    }

    public String generateRowKey(String carId, Long time, CstConstants.TIME_SELECT time_select) throws ParseException {
        return RowKeyGenerate.getRowKeyById(carId, time, time_select);
    }
}

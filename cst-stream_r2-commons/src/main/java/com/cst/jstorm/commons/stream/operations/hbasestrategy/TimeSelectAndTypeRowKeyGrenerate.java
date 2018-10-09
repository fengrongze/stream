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
public class TimeSelectAndTypeRowKeyGrenerate<S extends CSTData> implements IRowKeyGrenate<S>{
    Logger logger = LoggerFactory.getLogger(TimeSelectAndTypeRowKeyGrenerate.class);
    private String carId;

    private Long time;

    private String type;

    private CstConstants.TIME_SELECT time_select;

    private S s;

    public TimeSelectAndTypeRowKeyGrenerate(String carId, Long time,  CstConstants.TIME_SELECT time_select,String type) {
        this.carId = carId;
        this.time = time;
        this.type = type;
        this.time_select = time_select;
    }

    public TimeSelectAndTypeRowKeyGrenerate(CstConstants.TIME_SELECT time_select, S s, String type) {
        this.type = type;
        this.time_select = time_select;
        this.s = s;
    }

    @Override
    public String createrowkey() {
        return generateRowKey();
    }

    public String generateRowKey() {
        String rowKey="";
        try {
            if (s != null) {
                rowKey= generateRowKey(s.getCarId(), s.getTime(), time_select,type);
                logger.debug("create rowkey with s {},{},{},{} ",rowKey,s,time_select,type);
            }
            else {
                rowKey= generateRowKey(carId, time, time_select,type);
                logger.debug("create rowkey with no s {},{},{},{},{} ",rowKey,carId, time, time_select,type);
            }

            } catch (ParseException e) {
                logger.error("generate row key error:{},{},{}",carId,time,type,e);
            }
        return rowKey;
    }

    public String generateRowKey(String carId, Long time, CstConstants.TIME_SELECT time_select,String type) throws ParseException {
        String rowKey= RowKeyGenerate.getRowKeyById(carId, time, time_select,type);
        logger.debug("rowKey is {}",rowKey);
        return rowKey;
    }
}

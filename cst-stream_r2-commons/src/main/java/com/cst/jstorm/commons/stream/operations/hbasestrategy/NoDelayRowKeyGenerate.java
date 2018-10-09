package com.cst.jstorm.commons.stream.operations.hbasestrategy;

import com.cst.stream.common.RowKeyGenerate;
import com.cst.stream.stathour.CSTData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;

/**
 * @author Johnney.Chiu
 * create on 2018/5/8 11:57
 * @Description 临时数据对象
 * @title
 */
public class NoDelayRowKeyGenerate<T extends CSTData> implements IRowKeyGrenate<T> {
    private Logger logger = LoggerFactory.getLogger(NoDelayRowKeyGenerate.class);

    private T t;

    private String carId;

    private String type;

    public NoDelayRowKeyGenerate(T t, String type) {
        this.t = t;
        this.type = type;
    }

    public NoDelayRowKeyGenerate(String carId, String type) {
        this.carId = carId;
        this.type = type;
    }

    @Override
    public String createrowkey() {
        return generateRowKey();
    }

    public String generateRowKey() {
        try {
            if (t != null)
                return generateRowKey(t.getCarId(), type);
            else
                return generateRowKey(carId, type);

        } catch (ParseException e) {
            logger.error("generate no delay row key error:{},{}",carId,e);
        }
        return "";
    }

    public String generateRowKey(String carId, String type) throws ParseException {
        return RowKeyGenerate.getRowKeyById(carId, type);
    }
}

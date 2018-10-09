package com.cst.jstorm.commons.stream.operations.hbasestrategy;

import com.cst.stream.common.RowKeyGenerate;
import com.cst.stream.stathour.CSTData;

/**
 * @author Johnney.Chiu
 * create on 2018/3/1 11:35
 * @Description 无timeselecct的rowkey生成
 * @title
 */
public class GeneralRowkeyGenerate <T extends CSTData> implements IRowKeyGrenate<T>{

    private T t;

    private String carId;


    public GeneralRowkeyGenerate(T t) {
        this.t = t;
    }

    public GeneralRowkeyGenerate(String carId) {
        this.carId = carId;
    }

    @Override
    public String createrowkey() {
        if (t != null)
            return generateRowKey(t.getCarId());
        else
            return generateRowKey(carId);
    }

    private String generateRowKey(String carId) {
        return RowKeyGenerate.getRowKeyById(carId);
    }
}

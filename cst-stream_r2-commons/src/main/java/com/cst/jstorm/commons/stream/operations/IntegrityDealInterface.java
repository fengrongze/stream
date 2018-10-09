package com.cst.jstorm.commons.stream.operations;

import com.cst.stream.stathour.CSTData;

import java.util.Map;

/**
 * 完整性数据计算接口
 */
public interface IntegrityDealInterface<T extends CSTData>{

    /**
     * 将msg数据转换为map
     * @param t
     * @return
     */
     Map<String,String> convertData2Map(T t);

    /**
     * 将json数据转换为相应对象
     * @param msg
     * @return
     */
    T initMsg2Data(String msg);
}

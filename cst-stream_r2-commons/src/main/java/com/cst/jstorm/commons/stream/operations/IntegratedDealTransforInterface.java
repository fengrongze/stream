package com.cst.jstorm.commons.stream.operations;

import com.cst.jstorm.commons.stream.constants.ExceptionCodeStatus;
import com.cst.stream.stathour.CSTData;

import java.util.Map;

/**
 * 完整数据处理接口
 * @param <T>
 */
public interface IntegratedDealTransforInterface<T extends CSTData> {

     Map<String, String> convertData2Map(T transfor);
}

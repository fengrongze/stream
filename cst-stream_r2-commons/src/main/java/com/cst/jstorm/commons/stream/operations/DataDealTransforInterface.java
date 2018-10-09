package com.cst.jstorm.commons.stream.operations;

import com.cst.jstorm.commons.stream.constants.ExceptionCodeStatus;
import com.cst.stream.stathour.CSTData;
import com.cst.stream.stathour.dormancy.DormancySource;
import com.cst.stream.stathour.obd.ObdDayTransfor;

import java.util.Map;

/**
 * @author Johnney.chiu
 * create on 2017/12/1 10:38
 * @Description 处理数据的接口
 */
public interface DataDealTransforInterface<T extends CSTData,S extends CSTData,L extends CSTData> {

     /**
      * 比较上传数据是否合法
      * @param s
      * @param latestData
      * @param map
      * @return
      */
     ExceptionCodeStatus commpareExceptionWithEachData(S s, L latestData, Map map);

     /**
      * 根据原始数据和最新的计算中间结果数据
      * @param latestData
      * @param s
      * @param map
      * @return
      */
     L calcLatestData(L latestData, S s, Map map, ExceptionCodeStatus status);

    /**
     *
     * @param s
     * @param map
     * @param latestData
     * @return
     */
     L initLatestData(S s, Map map, L latestData);

    /**
     * 最新上传数据s与上个时区的第一条数据latestFirstData以及累计在缓存中的数据latestData做计算
      * @param latestFirstData
     * @param latestData
     * @param source
     * @param map
     * @return
     */
     T calcTransforData(S latestFirstData, L latestData, S source, Map map);

     /**
      * 以缓存中记录的累计数据做初始化,supplyTime为补充的时间。
      * @param latestData
      * @param source
      * @param map
      * @param supplyTime
      * @return
      */
     T initFromTempTransfer(L latestData,S source, Map map, long supplyTime);

    /**
     * 转换为map
     * @param transfor
     * @return
     */
     Map<String, String> convertData2Map(T transfor,L latestData);

    /**
     * 计算完整性数据
     * @param latestFirstData
     * @param latestData
     * @param map
     * @return
     */
    T calcIntegrityData(S latestFirstData, L latestData,T transfor ,Map map);


    /**
     * 根据休眠包数据获取最近数据初始化数据,supplyTime为补充的时间。
     * @param latestData
     * @param supplyTime
     * @return
     */
    T initFromDormancy(L latestData, long supplyTime);

}

package com.cst.jstorm.commons.stream.operations;

import com.cst.stream.stathour.CSTData;

import java.util.Map;

/**
 * @author yangxu
 * create on 2018/06/11 10:38
 * @Description 处理天数据的接口
 */

public interface DataAccumulationTransforInterface<T extends CSTData,S extends CSTData> {

     /**
      * 根据原始数据做初始化
      * @param s
      * @param map
      * @return
      */
     T initTransforDataBySource(S s, Map map);

    /**
     * 根据上一条统计天数据做初始化
     * @param latestTransforData
     * @param map
     * @return
     */
    T initTransforDataByLatest(T latestTransforData, Map map, Long time);

    /**
     * 最新上传数据s与累计在缓存中的数据latestData做计算
      * @param latestTransforData
     * @param source
     * @param map
     * @return
     */
     T calcTransforData(T latestTransforData, S source, Map map);

}

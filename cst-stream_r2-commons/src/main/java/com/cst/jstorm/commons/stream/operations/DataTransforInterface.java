package com.cst.jstorm.commons.stream.operations;

import com.cst.stream.stathour.CSTData;
import com.cst.stream.stathour.am.AmHourTransfor;
import com.cst.stream.stathour.dormancy.DormancySource;

import java.util.Map;

/**
 * @author Johnney.chiu
 * create on 2017/12/1 10:38
 * @Description 处理数据的接口
 */
public interface DataTransforInterface<T extends CSTData,S extends CSTData> {

     /**
      * 使用源计算当前的数据
      * @param t
      * @param s
      * @param other
      */
     void execute(T t, S s, Map<String, Object> other) ;

     /**
      * 根据已有的源记录初始化
      * @param s
      * @param other
      * @return
      */
     T init(S s, Map<String, Object> other);

     /**
      * 根据已有的源记录以及上一记录初始化
      * @param t
      * @param s
      * @param other
      * @return
      */
     T initOffet(T t, S s, Map<String, Object> other);

     /**
      * 根据中间计算结果初始化
      * @param t
      * @param other
      * @return
      */
     T initFromTransfer(T t, Map<String, Object> other);



}

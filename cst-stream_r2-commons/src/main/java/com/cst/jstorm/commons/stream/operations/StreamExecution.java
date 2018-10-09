package com.cst.jstorm.commons.stream.operations;

import com.cst.stream.stathour.CSTData;

import java.util.Map;

/**
 * @author Johnney.chiu
 * create on 2018/1/29 16:21
 * @Description 接口方法
 * @title
 */
public interface StreamExecution<T extends CSTData> {


    T findHBaseData(Map map);

    boolean persistHBaseData(Map map, T t);

    boolean persistHBaseData(Map map, String source);





}

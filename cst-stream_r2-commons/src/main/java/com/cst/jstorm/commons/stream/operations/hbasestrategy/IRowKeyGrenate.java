package com.cst.jstorm.commons.stream.operations.hbasestrategy;

/**
 * @author Johnney.Chiu
 * create on 2018/3/1 10:24
 * @Description rowkey的生成策略
 * @title
 */
public interface IRowKeyGrenate<T> {

    /*String generateRowKey(T t,CstConstants.TIME_SELECT time_select);

    String generateRowKey(T t);

    String generateRowKey(String carId,Long time,CstConstants.TIME_SELECT time_select);

    String generateRowKey(String key);*/

    String createrowkey();



}

package com.cst.jstorm.commons.stream.operations.hbasestrategy;

import java.util.Map;

/**
 * @author Johnney.chiu
 * create on 2018/1/29 18:22
 * @Description Data query or persist
 * @title
 */
public interface IHBaseQueryAndPersistStrategy<T> {
    T findHBaseData(Map map);

    boolean persistHBaseData(Map map, T t);

    boolean persistHBaseData(Map map, String source);

}

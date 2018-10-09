package com.cst.jstorm.commons.stream.operations.hbasestrategy;

import com.cst.jstorm.commons.utils.http.HttpUtils;

/**
 * @author Johnney.chiu
 * create on 2018/1/29 18:24
 * @Description http 策略
 * @title
 */
public interface IHBaseStrategyWithHttp<T> extends IHBaseQueryAndPersistStrategy<T> {

    //http client处理
    T findHBaseData(HttpUtils httpUtils, String url);

    boolean persistHBaseData(HttpUtils httpUtils, String msg, String url);
}

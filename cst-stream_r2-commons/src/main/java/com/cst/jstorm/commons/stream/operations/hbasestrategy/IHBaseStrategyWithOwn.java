package com.cst.jstorm.commons.stream.operations.hbasestrategy;

import org.apache.hadoop.hbase.client.Connection;

/**
 * @author Johnney.chiu
 * create on 2018/1/29 18:25
 * @Description http client 策略
 * @title
 */
public interface IHBaseStrategyWithOwn<T> extends IHBaseQueryAndPersistStrategy<T> {
    //hbase client 处理
    T findHBaseData(Connection connection, String tableName, String familyName,
                    String rowKey, Class<T> clazz, String... columns);


    boolean persistHBaseData(Connection connection, String tableName, String familyName,
                             String rowKey, T t);

    boolean persistHBaseData(Connection connection, String tableName, String familyName,
                             String rowKey, String source);

}

package com.cst.jstorm.commons.stream.operations;

import org.apache.hadoop.hbase.client.Result;

import java.util.List;

/**
 * @author Johnney.Chiu
 * create on 2018/9/17 15:52
 * @Description
 * @title
 */
public interface RowMappers<T> {
    List<T> mapRow(Result[] results, int rowNum) throws Exception;
}

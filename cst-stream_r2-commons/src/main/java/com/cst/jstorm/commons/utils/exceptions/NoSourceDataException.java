package com.cst.jstorm.commons.utils.exceptions;

/**
 * @author Johnney.chiu
 * create on 2017/12/1 11:22
 * @Description 没有源数据
 */
public class NoSourceDataException extends Throwable {

    public NoSourceDataException(String message) {
        super(message+",the source is null!");

    }
}

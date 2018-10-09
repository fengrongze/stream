/**
 * Company
 * Copyright (C) 2014-2017 All Rights Reserved.
 */
package com.cst.stream.common.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Johnney.chiu
 */
public class HbasePutBuilder<T> extends Put{

    private String colFamilyName;

    private Map<String, Object> fieldsMap;

    private Set<String> propertiesSet;

    public String getColFamilyName() {
        return colFamilyName;
    }

    public void setColFamilyName(String colFamilyName) {
        this.colFamilyName = colFamilyName;
    }

    /**
     * Create a Put operation for the specified row.
     *
     * @param row row key
     */
    public HbasePutBuilder(byte[] row) {
        super(row);
    }

    /**
     * Create a Put operation for the specified row, using a given timestamp.
     *
     * @param row row key; we make a copy of what we are passed to keep local.
     * @param ts  timestamp
     */
    public HbasePutBuilder(byte[] row, long ts) {
        super(row, ts);
    }

    /**
     * We make a copy of the passed in row key to keep local.
     *
     * @param rowArray
     * @param rowOffset
     * @param rowLength
     */
    public HbasePutBuilder(byte[] rowArray, int rowOffset, int rowLength) {
        super(rowArray, rowOffset, rowLength);
    }

    /**
     * @param row row key; we make a copy of what we are passed to keep local.
     * @param ts  timestamp
     */
    public HbasePutBuilder(ByteBuffer row, long ts) {
        super(row, ts);
    }

    /**
     * @param row row key; we make a copy of what we are passed to keep local.
     */
    public HbasePutBuilder(ByteBuffer row) {
        super(row);
    }

    /**
     * We make a copy of the passed in row key to keep local.
     *
     * @param rowArray
     * @param rowOffset
     * @param rowLength
     * @param ts
     */
    public HbasePutBuilder(byte[] rowArray, int rowOffset, int rowLength, long ts) {
        super(rowArray, rowOffset, rowLength, ts);
    }

    /**
     * Copy constructor.  Creates a Put operation cloned from the specified Put.
     *
     * @param putToCopy put to copy
     */
    public HbasePutBuilder(Put putToCopy) {
        super(putToCopy);
    }

    public HbasePutBuilder(String colFamilyName,byte[] row,T t) throws IllegalAccessException, IntrospectionException, InvocationTargetException {
        super(row);
        this.colFamilyName = colFamilyName;
        fieldsMap = new HashMap();
        propertiesSet = new HashSet<>();
        getPropertyMap(t);
    }

    private void getPropertyMap(T t) throws IntrospectionException, InvocationTargetException, IllegalAccessException {
        BeanInfo beanInfo = Introspector.getBeanInfo(t.getClass());
        PropertyDescriptor[] pds = beanInfo.getPropertyDescriptors();
        for(PropertyDescriptor pd : pds) {
            if(pd.getWriteMethod()==null)
                continue;
            String propertyName = pd.getName();
            Method methodGetX = pd.getReadMethod();
            this.build(HumpNameOrMethodUtils.humpEntityForVar(propertyName), methodGetX.invoke(t));
        }
    }

    public HbasePutBuilder build(String paramName, Object param){
        if(param==null)
            return null;
        this.addColumn(this.getColFamilyName().getBytes(), paramName.getBytes(),
                Bytes.toBytes(param.toString()));
        return this;
    }

}


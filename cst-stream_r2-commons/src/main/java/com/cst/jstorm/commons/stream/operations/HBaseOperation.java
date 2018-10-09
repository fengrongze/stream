package com.cst.jstorm.commons.stream.operations;

import com.cst.jstorm.commons.stream.operations.hbasestrategy.IRowKeyGrenate;
import com.cst.stream.common.hbase.HbaseFindBuilder;
import com.cst.stream.common.hbase.HbasePutBuilder;
import com.cst.stream.stathour.CSTData;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Johnney.chiu
 * create on 2018/1/25 14:28
 * @Description 操作流式计算的hbase表
 */
public class HBaseOperation<T extends CSTData> implements IRowKeyGrenate<T> {
    Logger logger = LoggerFactory.getLogger(HBaseOperation.class);

    private Connection connection;

    private String tableName;

    private String rowKey;

    private String familyName;

    private String[] columns;

    private Class<T> clazz;

    private T data;

    private IRowKeyGrenate<T> iRowKeyGrenate;

    private Map<String,String[]> familyQualifiers;

    public HBaseOperation() {
    }

    public HBaseOperation(Connection connection, String tableName) {
        this.connection = connection;
        this.tableName = tableName;
    }



    public HBaseOperation(Connection connection, String tableName, String familyName, T data) {
        this.connection = connection;
        this.tableName = tableName;
        this.familyName = familyName;
        this.data = data;
    }

    public HBaseOperation(Connection connection, String tableName, String familyName, String[] columns, Class<T> clazz) {
        this.connection = connection;
        this.tableName = tableName;
        this.familyName = familyName;
        this.columns = columns;
        this.clazz = clazz;
    }

    public HBaseOperation(Connection connection,String rowKey, String tableName, String familyName, String[] columns, Class<T> clazz) {
        this.connection = connection;
        this.rowKey = rowKey;
        this.tableName = tableName;
        this.familyName = familyName;
        this.columns = columns;
        this.clazz = clazz;
    }

    public HBaseOperation(Connection connection, String tableName, Map<String,String[]> familyQualifiers, Class<T> clazz) {
        this.connection = connection;
        this.tableName = tableName;
        this.familyQualifiers = familyQualifiers;
        this.clazz = clazz;
    }


    public boolean putCstData(){
        return putCstData(familyName, rowKey, data);
    }

    public boolean putCstData(T data){
        logger.debug("family is {},rowkey is {},data is {}",familyName,rowKey,data);
        return putCstData(familyName, rowKey, data);
    }


    @SuppressWarnings("unchecked")
    public boolean putCstData(String familyName, String rowKey, T t){
		long start = System.currentTimeMillis();
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));

            if(table==null) {
                logger.error("put hbase table error:{}", tableName);
                return false;
            }
            logger.debug("table is {},tablename is {},familyName is {},rowkey is {},data is {}",
                    table,tableName,familyName,rowKey,t);
	        Put put = new HbasePutBuilder(familyName,rowKey.getBytes(),t);
	        put.setDurability(Durability.ASYNC_WAL);
            table.put(put);
            return true;
        } catch (IOException e) {
            logger.error("put table{},data is {},rowkey is {},and  error:{}",tableName,t,rowKey,e);
        } catch (IllegalAccessException e) {
            logger.error("put table{},data is {},rowkey is {},and  error:{}",tableName,t,rowKey,e);
        } catch (IntrospectionException e) {
            logger.error("put table{},data is {},rowkey is {},and  error:{}",tableName,t,rowKey,e);
        } catch (InvocationTargetException e) {
            logger.error("put table{},data is {},rowkey is {},and  error:{}",tableName,t,rowKey,e);
        }finally {
            closeTable(table);
            logger.debug("HBasePut time cost {}ms", System.currentTimeMillis() - start);
        }
        return false;
    }

    public T getCstData(){
        if(null!=familyName){
            return getCstData(familyName, rowKey, clazz, columns);
        }else{
            return getCstData(familyQualifiers, rowKey, clazz);
        }

    }

    @SuppressWarnings("unchecked")
    public T getCstData(String familyName,String rowKey,Class<T> clazz,String...columns){
	    long start = System.currentTimeMillis();
        T t =getTableData(familyName, rowKey, null, new RowMapper<T>() {
            @Override
            public T mapRow(Result result, int rowNum) throws Exception {
                return (T)new HbaseFindBuilder(familyName, clazz).
                        build(result,columns).fetch();
            }
        });
	    logger.debug("HBaseGet time cost {}ms", System.currentTimeMillis() - start);
	    return t;
    }

    public T getCstData(Map<String,String[]> familyQualifiers,String rowKey,Class<T> clazz){
        long start = System.currentTimeMillis();
        T t =getTableData(familyName, rowKey, null, new RowMapper<T>() {
            @Override
            public T mapRow(Result result, int rowNum) throws Exception {
                return (T)new HbaseFindBuilder(familyQualifiers, clazz).
                        build(result).fetch();
            }
        });
        logger.debug("HBaseGet time cost {}ms", System.currentTimeMillis() - start);
        return t;
    }

    private T getTableData(String familyName,String rowKey,String qualifier,RowMapper<T> mapper){
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            if(table==null)
                logger.error("get hbase table error:{}",tableName);
            Get get = new Get(rowKey.getBytes(getCharset("")));
            if (familyName != null) {
                byte[] family = familyName.getBytes(getCharset(""));

                if (qualifier != null) {
                    get.addColumn(family, qualifier.getBytes(getCharset("")));
                }
                else {
                    get.addFamily(family);
                }
            }
            Result result = table.get(get);
            return mapper.mapRow(result, 0);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
           closeTable(table);
        }
        return null;
    }

    public List<T> getTableDataWithRowkeys(String familyName, List<String> rowKeys, String qualifier, RowMappers<T> mappers){
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            if(table==null)
                logger.error("get hbase table error:{}",tableName);
            List<Get> listGets = new ArrayList<Get>();
            for(String rowKey:rowKeys){
                Get get = new Get(rowKey.getBytes(getCharset("")));
                if (familyName != null) {
                    byte[] family = familyName.getBytes(getCharset(""));
                    if (qualifier != null) {
                        get.addColumn(family, qualifier.getBytes(getCharset("")));
                    }
                    else {
                        get.addFamily(family);
                    }
                }
                listGets.add(get);
            }

            Result[] results = table.get(listGets);
            return mappers.mapRow(results, 0);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeTable(table);
        }
        return null;
    }

    public List<T> getTableDataWithRowkeys( List<String> rowKeys, RowMappers<T> mappers){
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            if(table==null)
                logger.error("get hbase table error:{}",tableName);
            List<Get> listGets = new ArrayList<Get>();
            for(String rowKey:rowKeys){
                Get get = new Get(rowKey.getBytes(getCharset("")));
                listGets.add(get);
            }

            Result[] results = table.get(listGets);
            return mappers.mapRow(results, 0);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeTable(table);
        }
        return null;
    }

    private void closeTable(Table table){
        if(null!=table)
            try {
                table.close();
            } catch (IOException e) {
                logger.error("close table error:",e);
            }
    }

    Charset getCharset(String encoding) {
        return (StringUtils.hasText(encoding) ? Charset.forName(encoding) : Charset.forName("UTF-8"));
    }

    @Override
    public String createrowkey() {
        return iRowKeyGrenate.createrowkey();
    }

    public HBaseOperation<T> createiRowKeyGrenate(IRowKeyGrenate<T> iRowKeyGrenate) {
        this.iRowKeyGrenate = iRowKeyGrenate;
        return this;
    }

    public HBaseOperation<T> createRowKey(){
        rowKey = createrowkey();
        return this;
    }

    public HBaseOperation<T> createRowKey(String rowKey) {
        this.rowKey = rowKey;
        return this;
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public String getFamilyName() {
        return familyName;
    }

    public void setFamilyName(String familyName) {
        this.familyName = familyName;
    }

    public String[] getColumns() {
        return columns;
    }

    public void setColumns(String[] columns) {
        this.columns = columns;
    }

    public Class<T> getClazz() {
        return clazz;
    }

    public void setClazz(Class<T> clazz) {
        this.clazz = clazz;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }



}





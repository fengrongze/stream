package com.cst.bigdata.repository.hbase;

import com.cst.bigdata.domain.hbase.UserInfo;

import java.util.List;

/**
 * @author Johnney.chiu
 * create on 2017/11/23 16:10
 * @Description user mapper
 */
public interface HbaseAccountInfoMapper {

    UserInfo findUserInfoByEntity(String table, String family, String rowKey, UserInfo userInfo);

    List<UserInfo> findAll(String tablename, String family);

    void putEntity(String tableName,String family, String rowKey,UserInfo userInfo);

}

package com.cst.bigdata.service.hbase;

import com.cst.bigdata.domain.hbase.UserInfo;

import java.util.List;

/**
 * @author Johnney.chiu
 * create on 2017/11/23 16:22
 * @Description Hbase account service interface
 */
public interface HbaseAccountInfoService {
    UserInfo findUserInfoByEntity(String table, String family, String rowKey, UserInfo userInfo);

    List<UserInfo> findAll(String tablename, String family);

    void putAccount(String table, String family, String rowKey, UserInfo userInfo);

}

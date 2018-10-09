package com.cst.bigdata.service.hbase.impl;

import com.cst.bigdata.domain.hbase.UserInfo;
import com.cst.bigdata.repository.hbase.HbaseAccountInfoMapper;
import com.cst.bigdata.service.hbase.HbaseAccountInfoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author Johnney.chiu
 * create on 2017/11/23 16:23
 * @Description HbaseAccountInfoServiceImpl
 */
@Service
public class HbaseAccountInfoServiceImpl implements HbaseAccountInfoService {

    @Autowired
    HbaseAccountInfoMapper hbaseAccountInfoMapper;

    @Override
    public UserInfo findUserInfoByEntity(String table, String family, String rowKey, UserInfo userInfo) {
        return hbaseAccountInfoMapper.findUserInfoByEntity(table,family,rowKey,userInfo);
    }

    @Override
    public List<UserInfo> findAll(String tablename, String family) {
        return hbaseAccountInfoMapper.findAll(tablename,family);
    }

    @Override
    public void putAccount(String table, String family, String rowKey, UserInfo userInfo) {
        hbaseAccountInfoMapper.putEntity(table,family,rowKey,userInfo);
    }
}

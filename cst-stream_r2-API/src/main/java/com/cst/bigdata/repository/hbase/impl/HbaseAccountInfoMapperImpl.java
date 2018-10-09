package com.cst.bigdata.repository.hbase.impl;

import com.cst.bigdata.domain.hbase.UserInfo;
import com.cst.bigdata.repository.hbase.HbaseAccountInfoMapper;
import com.cst.stream.common.hbase.HbaseFindBuilder;
import com.cst.stream.common.hbase.HbasePutBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * @author Johnney.chiu
 * create on 2017/11/23 16:17
 * @Description for implement mapper
 */
@Repository
public class HbaseAccountInfoMapperImpl implements HbaseAccountInfoMapper {

    @Autowired
    private HbaseTemplate hbaseTemplate;

    @Override
    public UserInfo findUserInfoByEntity(String table, String family, String rowKey, UserInfo userInfo) {
        return (UserInfo) hbaseTemplate.get(table, rowKey, family,
                (result, rowNum) -> new HbaseFindBuilder<>(family,  userInfo.getClass()).build(result,"userName","age","id").fetch());

    }

    @Override
    public void putEntity(String tableName,String family,String rowKey, UserInfo userInfo) {
        hbaseTemplate.execute(tableName, (table) -> {
            HbasePutBuilder<UserInfo> hbasePutBuilder = new HbasePutBuilder(family,rowKey.getBytes(),userInfo);
            table.put(hbasePutBuilder);
            return true;
        });
    }

    @Override
    public List<UserInfo> findAll(String tablename, String family) {
        byte[] cf_info = family.getBytes();
        byte[] age_info = Bytes.toBytes("age");
        byte[] id_info = Bytes.toBytes("id");
        byte[] username_info = Bytes.toBytes("userName");
        return hbaseTemplate.find(tablename, family,(result, rowNum) -> {
                UserInfo  u = new UserInfo();
                u.setId(Bytes.toString(result.getValue(cf_info,id_info)));
                u.setUserName(Bytes.toString(result.getValue(cf_info,username_info)));
                u.setAge(Bytes.toInt(result.getValue(cf_info,age_info)));
                return u;
        });
    }
}

package com.cst.bigdata.repository.hbase;


import com.cst.stream.common.hbase.HbaseFindBuilder;
import com.cst.stream.common.hbase.HbasePutBuilder;
import com.cst.stream.stathour.CSTData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.stereotype.Repository;

/**
 * @author Johnney.chiu
 * create on 2017/12/19 18:11
 * @Description 小时数据查询统计mapper
 */
@Repository
public class CstStreamHourSourceDataMapper<S extends CSTData> {

    @Autowired
    private HbaseTemplate hbaseTemplate;

    @SuppressWarnings("unchecked")
    public S findSourceDataByRowKey(String tableName, String familyName, String rowKey,String columns[], Class<?> clazz) {
        return (S) hbaseTemplate.get(tableName, rowKey, familyName, (result, rowNum) ->
                new HbaseFindBuilder(familyName,clazz).build(result,columns)).fetch();
    }

    @SuppressWarnings("unchecked")
    public void putSourceData(String tableName,String family,String rowKey, S sourceData) {
        hbaseTemplate.execute(tableName, (table) -> {
            HbasePutBuilder<S> hbasePutBuilder = new HbasePutBuilder(family,rowKey.getBytes(),sourceData);
            table.put(hbasePutBuilder);
            return true;
        });
    }

}

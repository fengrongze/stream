package com.cst.bigdata.repository.hbase;


import com.cst.jstorm.commons.stream.operations.HBaseOperation;
import com.cst.stream.common.hbase.HbaseFindBuilder;
import com.cst.stream.common.hbase.HbasePutBuilder;
import com.cst.stream.stathour.CSTData;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.stereotype.Repository;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Johnney.chiu
 * create on 2017/12/19 18:11
 * @Description 小时数据查询统计mapper
 */
@Repository
@Slf4j
public class CstStreamDayStatisticsMapper<T extends CSTData> {

    @Autowired
    private HbaseTemplate hbaseTemplate;

    @Autowired
    private Connection connection;


    public T findDayTransforByRowKey(String tableName, String familyName, String rowKey,String columns[], Class<?> clazz) {
        return (T) hbaseTemplate.get(tableName, rowKey, familyName, (result, rowNum) ->
                new HbaseFindBuilder(familyName,  clazz).build(result,columns)).fetch();
    }
    public T findDayTransforByRowKey(String tableName, Map familyQulifiers, String rowKey, String columns[], Class<?> clazz) {
        return (T) hbaseTemplate.get(tableName, rowKey, (result, rowNum) ->
                new HbaseFindBuilder(familyQulifiers, clazz).build(result)).fetch();
    }

    public void putDayTransfor(String tableName,String family,String rowKey, T data) {
        hbaseTemplate.execute(tableName, (table) -> {
            HbasePutBuilder<T> hbasePutBuilder = new HbasePutBuilder(family,rowKey.getBytes(),data);
            table.put(hbasePutBuilder);
            return true;
        });
    }

    public <T extends CSTData> List<T> findDayTransforByScan(String tableName, Map<String, String[]> familyMap, String fromRowKey, String toRowKey, String[] allColumns, Class<?> clazz) {
        Scan scan = new Scan();
        scan.setCaching(200);
        scan.setCaching(5000);
        scan.setStartRow(fromRowKey.getBytes());
        Filter filter = new InclusiveStopFilter(toRowKey.getBytes());
        scan.setFilter(filter);
        HbaseFindBuilder hbaseFindBuilder=new HbaseFindBuilder();
        return hbaseTemplate.find(tableName, scan, (result, rowNum) ->
            (T)hbaseFindBuilder.buildWithOwnScanMap(result, familyMap, clazz));


    }

    public <T extends CSTData> List<T> findDayTransforByRowKeys(String tableName, Map<String, String[]> familyMap, List<String> rowKeys, Class<?> clazz) {

        HBaseOperation hBaseOperation = new HBaseOperation(connection, tableName);
        return hBaseOperation.getTableDataWithRowkeys(rowKeys, (results, i) -> {
           List<T> ls=Arrays.asList(results).stream().filter(result -> result != null && !result.isEmpty())
                    .map(result -> (T) new HbaseFindBuilder().buildWithOwnScanMap(result, familyMap, clazz))
                    .collect(Collectors.toList());
            return ls;
        });
    }
}

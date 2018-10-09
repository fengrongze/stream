package com.cst.bigdata.repository.hbase;

import com.cst.stream.common.HbaseColumn;
import com.cst.stream.common.hbase.HbaseFindBuilder;
import com.cst.stream.common.hbase.HbasePutBuilder;
import com.cst.stream.stathour.am.AmDayTransfor;
import com.cst.stream.stathour.de.DeDayTransfor;
import com.cst.stream.stathour.gps.GpsDayTransfor;
import com.cst.stream.stathour.obd.ObdDayTransfor;
import com.cst.stream.stathour.trace.TraceDayTransfor;
import com.cst.stream.stathour.tracedelete.TraceDeleteDayTransfor;
import com.cst.stream.stathour.voltage.VoltageDayTransfor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.stereotype.Repository;

/**
 * @author Johnney.Chiu
 * create on 2018/3/6 15:02
 * @Description cst stream day no delay mappers
 * @title
 */

@Repository
public class CstStreamDayNoDelayStatitisticsMapper {

    @Autowired
    private HbaseTemplate hbaseTemplate;

    public AmDayTransfor findAmDayNoDelayTransforByRowKey(String tableName, String familyName, String rowKey, Class<?> clazz) {
        return (AmDayTransfor) hbaseTemplate.get(tableName, rowKey, familyName, (result, rowNum) ->
                new HbaseFindBuilder(familyName, clazz).build( result,HbaseColumn.DayNoDelayStatisticsCloumn.amDayColumns)).fetch();
    }

    public void putAmDayNoDelayTransfor(String tableName,String family,String rowKey, AmDayTransfor amDayTransfor) {
        hbaseTemplate.execute(tableName, (table) -> {
            HbasePutBuilder<AmDayTransfor> hbasePutBuilder = new HbasePutBuilder(family,rowKey.getBytes(),amDayTransfor);
            table.put(hbasePutBuilder);
            return true;
        });
    }

    public DeDayTransfor findDeDayNoDelayTransforByRowKey(String tableName, String familyName, String rowKey, Class<?> clazz) {
        return (DeDayTransfor) hbaseTemplate.get(tableName, rowKey, familyName, (result, rowNum) ->
                new HbaseFindBuilder(familyName,  clazz).build(result,HbaseColumn.DayNoDelayStatisticsCloumn.deDayColumns)).fetch();
    }

    public void putDeDayNoDelayTransfor(String tableName,String family,String rowKey, DeDayTransfor deDayTransfor) {
        hbaseTemplate.execute(tableName, (table) -> {
            HbasePutBuilder<AmDayTransfor> hbasePutBuilder = new HbasePutBuilder(family,rowKey.getBytes(),deDayTransfor);
            table.put(hbasePutBuilder);
            return true;
        });
    }

    public GpsDayTransfor findGpsDayNoDelayTransforByRowKey(String tableName, String familyName, String rowKey, Class<?> clazz) {
        return (GpsDayTransfor) hbaseTemplate.get(tableName, rowKey, familyName, (result, rowNum) ->
                new HbaseFindBuilder(familyName,clazz).build( result, HbaseColumn.DayNoDelayStatisticsCloumn.gpsDayColumns)).fetch();
    }

    public void putGpsDayNoDelayTransfor(String tableName,String family,String rowKey, GpsDayTransfor gpsDayTransfor) {
        hbaseTemplate.execute(tableName, (table) -> {
            HbasePutBuilder<GpsDayTransfor> hbasePutBuilder = new HbasePutBuilder(family,rowKey.getBytes(),gpsDayTransfor);
            table.put(hbasePutBuilder);
            return true;
        });
    }

    public ObdDayTransfor findObdDayNoDelayTransforByRowKey(String tableName, String familyName, String rowKey, Class<?> clazz) {
        return (ObdDayTransfor) hbaseTemplate.get(tableName, rowKey, familyName, (result, rowNum) ->
                new HbaseFindBuilder<>(familyName,clazz).build( result, HbaseColumn.DayNoDelayStatisticsCloumn.obdDayColumns)).fetch();
    }

    public void putObdDayNoDelayTransfor(String tableName,String family,String rowKey, ObdDayTransfor obdDayTransfor) {
        hbaseTemplate.execute(tableName, (table) -> {
            HbasePutBuilder<ObdDayTransfor> hbasePutBuilder = new HbasePutBuilder(family,rowKey.getBytes(),obdDayTransfor);
            table.put(hbasePutBuilder);
            return true;
        });
    }

    public TraceDeleteDayTransfor findTraceDeleteDayNoDelayTransforByRowKey(String tableName, String familyName, String rowKey, Class<?> clazz) {
        return (TraceDeleteDayTransfor) hbaseTemplate.get(tableName, rowKey, familyName, (result, rowNum) ->
                new HbaseFindBuilder<>(familyName, clazz).build( result,HbaseColumn.DayNoDelayStatisticsCloumn.traceDeleteDayColumns)).fetch();

    }

    public void putTraceDeleteDayNoDelayTransfor(String tableName,String family,String rowKey,  TraceDeleteDayTransfor traceDeleteDayTransfor) {
        hbaseTemplate.execute(tableName, (table) -> {
            HbasePutBuilder<ObdDayTransfor> hbasePutBuilder = new HbasePutBuilder(family,rowKey.getBytes(),traceDeleteDayTransfor);
            table.put(hbasePutBuilder);
            return true;
        });
    }

    public TraceDayTransfor findTraceDayNoDelayTransforByRowKey(String tableName, String familyName, String rowKey, Class<?> clazz) {
        return (TraceDayTransfor) hbaseTemplate.get(tableName, rowKey, familyName, (result, rowNum) ->
                new HbaseFindBuilder<>(familyName, clazz).build( result,HbaseColumn.DayNoDelayStatisticsCloumn.traceDayColumns)).fetch();

    }

    public void putTraceDayNoDelayTransfor(String tableName,String family,String rowKey, TraceDayTransfor traceDayTransfor) {
        hbaseTemplate.execute(tableName, (table) -> {
            HbasePutBuilder<ObdDayTransfor> hbasePutBuilder = new HbasePutBuilder(family,rowKey.getBytes(),traceDayTransfor);
            table.put(hbasePutBuilder);
            return true;
        });
    }

    public VoltageDayTransfor findVoltageDayNoDelayTransforByRowKey(String tableName, String familyName, String rowKey, Class<?> clazz) {
        return (VoltageDayTransfor) hbaseTemplate.get(tableName, rowKey, familyName, (result, rowNum) ->
                new HbaseFindBuilder<>(familyName,  clazz).build(result,HbaseColumn.DayNoDelayStatisticsCloumn.voltageDayColumns)).fetch();

    }

    public void putVoltageDayNoDelayTransfor(String tableName,String family,String rowKey,  VoltageDayTransfor voltageDayTransfor) {
        hbaseTemplate.execute(tableName, (table) -> {
            HbasePutBuilder<ObdDayTransfor> hbasePutBuilder = new HbasePutBuilder(family,rowKey.getBytes(),voltageDayTransfor);
            table.put(hbasePutBuilder);
            return true;
        });
    }
}

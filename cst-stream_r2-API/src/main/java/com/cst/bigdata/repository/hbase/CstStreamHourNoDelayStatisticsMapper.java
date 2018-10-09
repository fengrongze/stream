package com.cst.bigdata.repository.hbase;


import com.cst.stream.common.HbaseColumn;
import com.cst.stream.common.hbase.HbaseFindBuilder;
import com.cst.stream.common.hbase.HbasePutBuilder;
import com.cst.stream.stathour.am.AmHourTransfor;
import com.cst.stream.stathour.de.DeHourTransfor;
import com.cst.stream.stathour.gps.GpsHourTransfor;
import com.cst.stream.stathour.obd.ObdHourTransfor;
import com.cst.stream.stathour.trace.TraceHourTransfor;
import com.cst.stream.stathour.tracedelete.TraceDeleteHourTransfor;
import com.cst.stream.stathour.voltage.VoltageHourTransfor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.stereotype.Repository;

/**
 * @author Johnney.chiu
 * create on 2018/3/6 15:02
 * @Description 小时数据 nodelay 统计mapper
 */
@Repository
public class CstStreamHourNoDelayStatisticsMapper {

    @Autowired
    private HbaseTemplate hbaseTemplate;

    @SuppressWarnings("unchecked")
    public AmHourTransfor findAmNoDelayHourTransforByRowKey(String tableName, String familyName, String rowKey, Class<?> clazz) {
        return (AmHourTransfor) hbaseTemplate.get(tableName, rowKey, familyName, (result, rowNum) ->
                new HbaseFindBuilder(familyName, clazz).build( result,HbaseColumn.HourNoDelayStatisticsCloumn.amHourColumns)).fetch();
    }

    @SuppressWarnings("unchecked")
    public void putAmNoDelayHourTransfor(String tableName,String family,String rowKey, AmHourTransfor amHourTransfor) {
        hbaseTemplate.execute(tableName, (table) -> {
            HbasePutBuilder<AmHourTransfor> hbasePutBuilder = new HbasePutBuilder(family,rowKey.getBytes(),amHourTransfor);
            table.put(hbasePutBuilder);
            return true;
        });
    }

    @SuppressWarnings("unchecked")
    public DeHourTransfor findDeNoDelayHourTransforByRowKey(String tableName, String familyName, String rowKey, Class<?> clazz) {
        return (DeHourTransfor) hbaseTemplate.get(tableName, rowKey, familyName, (result, rowNum) ->
                new HbaseFindBuilder(familyName, clazz).build( result,HbaseColumn.HourNoDelayStatisticsCloumn.deHourColumns)).fetch();
    }

    @SuppressWarnings("unchecked")
    public void putDeNoDelayHourTransfor(String tableName,String family,String rowKey, DeHourTransfor deHourTransfor) {
        hbaseTemplate.execute(tableName, (table) -> {
            HbasePutBuilder<DeHourTransfor> hbasePutBuilder = new HbasePutBuilder(family,rowKey.getBytes(),deHourTransfor);
            table.put(hbasePutBuilder);
            return true;
        });
    }

    @SuppressWarnings("unchecked")
    public GpsHourTransfor findGpsNoDelayHourTransforByRowKey(String tableName, String familyName, String rowKey, Class<?> clazz) {
        return (GpsHourTransfor) hbaseTemplate.get(tableName, rowKey, familyName, (result, rowNum) ->
                new HbaseFindBuilder(familyName,  clazz).build(result,HbaseColumn.HourNoDelayStatisticsCloumn.gpsHourColumns)).fetch();
    }

    @SuppressWarnings("unchecked")
    public void putGpsNoDelayHourTransfor(String tableName,String family,String rowKey, GpsHourTransfor gpsHourTransfor) {
        hbaseTemplate.execute(tableName, (table) -> {
            HbasePutBuilder<GpsHourTransfor> hbasePutBuilder = new HbasePutBuilder(family,rowKey.getBytes(),gpsHourTransfor);
            table.put(hbasePutBuilder);
            return true;
        });
    }

    @SuppressWarnings("unchecked")
    public ObdHourTransfor findObdNoDelayHourTransforByRowKey(String tableName, String familyName, String rowKey, Class<?> clazz) {
        return (ObdHourTransfor) hbaseTemplate.get(tableName, rowKey, familyName, (result, rowNum) ->
                new HbaseFindBuilder(familyName, clazz).build(result, HbaseColumn.HourNoDelayStatisticsCloumn.obdHourColumns)).fetch();
    }

    public void putObdNoDelayHourTransfor(String tableName,String family,String rowKey, ObdHourTransfor obdHourTransfor) {
        hbaseTemplate.execute(tableName, (table) -> {
            HbasePutBuilder<ObdHourTransfor> hbasePutBuilder = new HbasePutBuilder(family,rowKey.getBytes(),obdHourTransfor);
            table.put(hbasePutBuilder);
            return true;
        });
    }

    public TraceHourTransfor findTraceNoDelayHourTransforByRowKey(String tableName, String familyName, String rowKey, Class<?> clazz) {
        return (TraceHourTransfor) hbaseTemplate.get(tableName, rowKey, familyName, (result, rowNum) ->
                new HbaseFindBuilder(familyName,  clazz).build(result,HbaseColumn.HourNoDelayStatisticsCloumn.traceHourColumns)).fetch();

    }

    public void putTraceNoDelayHourTransfor(String tableName,String family,String rowKey, TraceHourTransfor traceHourTransfor) {
        hbaseTemplate.execute(tableName, (table) -> {
            HbasePutBuilder<ObdHourTransfor> hbasePutBuilder = new HbasePutBuilder(family,rowKey.getBytes(),traceHourTransfor);
            table.put(hbasePutBuilder);
            return true;
        });

    }

    public TraceDeleteHourTransfor findTraceDeleteNoDelayHourTransforByRowKey(String tableName, String familyName, String rowKey, Class<?> clazz) {
        return (TraceDeleteHourTransfor) hbaseTemplate.get(tableName, rowKey, familyName, (result, rowNum) ->
                new HbaseFindBuilder(familyName,  clazz).build(result,HbaseColumn.HourNoDelayStatisticsCloumn.traceDeleteHourColumns)).fetch();

    }

    public void putTraceDeleteNoDelayHourTransfor(String tableName,String family,String rowKey, TraceDeleteHourTransfor traceDeleteHourTransfor) {
        hbaseTemplate.execute(tableName, (table) -> {
            HbasePutBuilder<ObdHourTransfor> hbasePutBuilder = new HbasePutBuilder(family,rowKey.getBytes(),traceDeleteHourTransfor);
            table.put(hbasePutBuilder);
            return true;
        });
    }

    public VoltageHourTransfor findVoltageNoDelayHourTransforByRowKey(String tableName, String familyName, String rowKey, Class<?> clazz) {
        return (VoltageHourTransfor) hbaseTemplate.get(tableName, rowKey, familyName, (result, rowNum) ->
                new HbaseFindBuilder(familyName, clazz).build( result,HbaseColumn.HourNoDelayStatisticsCloumn.voltageHourColumns)).fetch();

    }

    public void putVoltageNoDelayHourTransfor(String tableName,String family,String rowKey, VoltageHourTransfor voltageHourTransfor) {
        hbaseTemplate.execute(tableName, (table) -> {
            HbasePutBuilder<ObdHourTransfor> hbasePutBuilder = new HbasePutBuilder(family,rowKey.getBytes(),voltageHourTransfor);
            table.put(hbasePutBuilder);
            return true;
        });
    }
}

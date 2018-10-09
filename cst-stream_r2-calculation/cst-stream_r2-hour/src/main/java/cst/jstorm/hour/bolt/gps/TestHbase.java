package cst.jstorm.hour.bolt.gps;

import com.cst.stream.common.*;
import com.cst.stream.common.hbase.HbasePutBuilder;
import com.cst.stream.stathour.CSTData;
import com.cst.stream.stathour.am.AmHourSource;
import com.cst.stream.stathour.gps.GpsDayTransfor;
import com.cst.jstorm.commons.stream.operations.HBaseOperation;
import com.cst.jstorm.commons.utils.HBaseConnectionUtil;
import com.cst.jstorm.commons.utils.spring.MyApplicationContext;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.springframework.context.support.AbstractApplicationContext;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Johnney.chiu
 * create on 2018/1/25 11:32
 * @Description
 */
public class TestHbase<T extends CSTData> {

    private T t;

    private Connection hBaseConnection;
    @SuppressWarnings("unchecked")
    public static void main(String... args) throws ParseException {
        long sysTime = System.currentTimeMillis();
        Connection connection = null;
        try {
           /* connection = HBaseConnectionUtil.
                    createHBaseConnnection("172.31.15.75,172.31.15.76,172.31.15.77", "2181",
                            "","/hbase-unsecure");*/
            HBaseConnectionUtil.HbaseConnectionProperties properties
                    = new HBaseConnectionUtil.HbaseConnectionProperties(
                    "", "172.31.15.75,172.31.15.76,172.31.15.77",
                    "2181", "", "/hbase-unsecure", "8","8","8");
            connection = HBaseConnectionUtil.createHBaseConnnection(properties);
            Table table=connection.getTable(TableName.valueOf(HBaseTable.HOUR_FIRST_ZONE.getTableName()));
            GpsDayTransfor gpsDayTransfor = new GpsDayTransfor();
            gpsDayTransfor.setCarId("13344444");

            gpsDayTransfor.setTime(sysTime);
            gpsDayTransfor.setGpsCount(11);
            String rowKey = RowKeyGenerate.getRowKeyById(gpsDayTransfor.getCarId(),gpsDayTransfor.getTime(), CstConstants.TIME_SELECT.DAY);
            HbasePutBuilder<GpsDayTransfor> hbasePutBuilder =
                    new HbasePutBuilder(HBaseTable.DAY_STATISTICS.getFirstFamilyName(),rowKey.getBytes(),gpsDayTransfor);
            //table.put(hbasePutBuilder);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (IntrospectionException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }


        HBaseOperation<AmHourSource> hBaseOperation = new HBaseOperation<>(connection, HBaseTable.HOUR_FIRST_ZONE.getTableName());
        String rowKey = RowKeyGenerate.getRowKeyById("68cabd75469c4ecd97070f9a7506272a", 1528884251000L,CstConstants.TIME_SELECT.HOUR, StreamTypeDefine.GPS_TYPE);
        AmHourSource amHourSource=hBaseOperation.getCstData(HBaseTable.HOUR_FIRST_ZONE.getFirstFamilyName(),rowKey,AmHourSource.class,HbaseColumn.HourSourceColumn.amHourColumns);
        System.out.println(amHourSource);

        /*TestHbase<GpsDayTransfor> gpsDayTransforTestHbase = new TestHbase<>();
        AbstractApplicationContext context = MyApplicationContext.getDefaultContext();
        gpsDayTransforTestHbase.sethBaseConnection((Connection)context.getBean("hBaseConnection"));
        Map<String, Object> map = new HashMap<>();
        map.put("rowKey", rowKey);
        map.put("tableName", HBaseTable.DAY_STATISTICS.getTableName());
        map.put("familyName", HBaseTable.DAY_STATISTICS.getFirstFamilyName());
        map.put("columns", HbaseColumn.DayStatisticsCloumn.gpsDayColumns);
        map.put("class", GpsDayTransfor.class);
        System.out.println(gpsDayTransforTestHbase.getData(map).getGpsCount());
        context.close();
        */

    }
    @SuppressWarnings("unchecked")
    public T getData(Map map){
        HBaseOperation<T> hBaseOperation = new HBaseOperation<>(hBaseConnection, (String)map.get("tableName"));
        t=hBaseOperation.getCstData((String)map.get("familyName"),(String)map.get("rowKey"), (Class<T>)map.get("class"), (String[])map.get("columns"));
        return t;
    }

    public T getT() {
        return t;
    }

    public void setT(T t) {
        this.t = t;
    }

    public Connection gethBaseConnection() {
        return hBaseConnection;
    }

    public void sethBaseConnection(Connection hBaseConnection) {
        this.hBaseConnection = hBaseConnection;
    }
}

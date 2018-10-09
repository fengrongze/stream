package cst.jstorm.daymonth.test;

import com.cst.jstorm.commons.stream.constants.OtherKey;
import com.cst.jstorm.commons.stream.custom.CustomContextConfiguration;
import com.cst.jstorm.commons.stream.operations.hbasestrategy.IHBaseQueryAndPersistStrategy;
import com.cst.jstorm.commons.stream.operations.hbasestrategy.StrategyChoose;
import com.cst.jstorm.commons.stream.operations.hbasestrategy.TimeSelectRowKeyGrenerate;
import com.cst.jstorm.commons.utils.spring.MyApplicationContext;
import com.cst.stream.common.CstConstants;
import com.cst.stream.common.HBaseTable;
import com.cst.stream.common.HbaseColumn;
import com.cst.stream.common.JsonHelper;
import com.cst.stream.stathour.CSTData;
import com.cst.stream.stathour.integrated.DayIntegratedTransfor;
import com.cst.stream.stathour.obd.ObdDayTransfor;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.gson.Gson;
import cst.jstorm.daymonth.calcalations.integrated.DayIntegratedCalcBiz;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.util.StringUtils;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
public class HbaseTest {
     static DayIntegratedCalcBiz dayIntegratedCalcBiz = new DayIntegratedCalcBiz();
    private  static Gson gson = new Gson();
    public static void main(String[] args) {
        try {
            AbstractApplicationContext beanContext = MyApplicationContext.getDefineContextWithHttpUtil(CustomContextConfiguration.class);
            Connection connection = (Connection) beanContext.getBean(OtherKey.DataDealKey.HBASE_CONNECTION);
            /*ObdDayTransfor obdDayTransfor = JsonHelper.toBeanWithoutException(msg, new TypeReference<ObdDayTransfor>() {
            });*/
            Map<String,String[]> familyQualifiers = createFamilyQualifiers();
            IHBaseQueryAndPersistStrategy<DayIntegratedTransfor> iDayIntegratedStrategy =
                    StrategyChoose.generateStrategy(
                            connection, HBaseTable.DAY_STATISTICS.getTableName(),
                            familyQualifiers,
                            DayIntegratedTransfor.class);

            ObdDayTransfor obdDayTransfor = new ObdDayTransfor();
            obdDayTransfor.setCarId("M201600010008");
            obdDayTransfor.setTime(1534338433000L);
            Map<String,Object> map = new HashMap<>();
            DayIntegratedTransfor dayIntegratedTransfor = findHbase(map,iDayIntegratedStrategy,obdDayTransfor);

            System.out.println("hbase查询天数据:" + gson.toJson(dayIntegratedCalcBiz.convertData2Map(dayIntegratedTransfor)) + "");
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private static Map<String,String[]> createFamilyQualifiers() {
        Map<String,String[]> familyQualifiers = new HashMap<>();
        familyQualifiers.put(HBaseTable.DAY_STATISTICS.getFirstFamilyName(),HbaseColumn.DayStatisticsCloumn.obdDayColumns);
        familyQualifiers.put(HBaseTable.DAY_STATISTICS.getThirdFamilyName(),HbaseColumn.DayStatisticsCloumn.amDayColumns);
        familyQualifiers.put(HBaseTable.DAY_STATISTICS.getFourthFamilyName(),HbaseColumn.DayStatisticsCloumn.deDayColumns);
        return familyQualifiers;
    }

    private static   <K extends CSTData,N extends CSTData> K findHbase(Map map, IHBaseQueryAndPersistStrategy<K> ihBaseQueryAndPersistStrategy, N needData){
        CstConstants.TIME_SELECT timeSelect= CstConstants.TIME_SELECT.DAY;
        TimeSelectRowKeyGrenerate timeSelectRowKeyGrenerate = new TimeSelectRowKeyGrenerate(needData.getCarId(),
                needData.getTime(),
                timeSelect);
        System.out.println("rowkey:"+timeSelectRowKeyGrenerate.generateRowKey());
        map.put(OtherKey.DataDealKey.ROWKEY_GENERATE, timeSelectRowKeyGrenerate);
        return ihBaseQueryAndPersistStrategy.findHBaseData(map);
    }
    private static Charset  getCharset(String encoding) {
        return (StringUtils.hasText(encoding) ? Charset.forName(encoding) : Charset.forName("UTF-8"));
    }
}

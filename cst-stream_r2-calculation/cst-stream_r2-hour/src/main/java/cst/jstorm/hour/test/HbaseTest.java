package cst.jstorm.hour.test;

import com.cst.jstorm.commons.stream.constants.OtherKey;
import com.cst.jstorm.commons.stream.custom.CustomContextConfiguration;
import com.cst.jstorm.commons.stream.operations.IntegratedDealTransforInterface;
import com.cst.jstorm.commons.stream.operations.hbasestrategy.IHBaseQueryAndPersistStrategy;
import com.cst.jstorm.commons.stream.operations.hbasestrategy.StrategyChoose;
import com.cst.jstorm.commons.stream.operations.hbasestrategy.TimeSelectRowKeyGrenerate;
import com.cst.jstorm.commons.utils.spring.MyApplicationContext;
import com.cst.stream.common.CstConstants;
import com.cst.stream.common.HBaseTable;
import com.cst.stream.common.HbaseColumn;
import com.cst.stream.common.JsonHelper;
import com.cst.stream.stathour.CSTData;
import com.cst.stream.stathour.integrated.HourIntegratedTransfor;
import com.cst.stream.stathour.obd.ObdHourTransfor;
import com.fasterxml.jackson.core.type.TypeReference;
import cst.jstorm.hour.calcalations.integrated.HourIntegratedCalcBiz;
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
    public static void main(String[] args) {
        try {
            AbstractApplicationContext beanContext = MyApplicationContext.getDefineContextWithHttpUtil(CustomContextConfiguration.class);
            Connection connection = (Connection) beanContext.getBean(OtherKey.DataDealKey.HBASE_CONNECTION);
            ObdHourTransfor obdHourTransfor = new ObdHourTransfor();
            obdHourTransfor.setCarId("c0dca07efafd4e06825f52e0250925fe");
            obdHourTransfor.setTime(1534305161000L);
            Map<String,String[]> familyQualifiers = createFamilyQualifiers();
            IHBaseQueryAndPersistStrategy<HourIntegratedTransfor> iHourIntegratedStrategy =
                    StrategyChoose.generateStrategy(
                            connection, HBaseTable.HOUR_STATISTICS.getTableName(),
                            familyQualifiers,
                            HourIntegratedTransfor.class);
            Map<String,Object> map = new HashMap<>();
            HourIntegratedTransfor hourIntegratedTransfor = findHbase(map,iHourIntegratedStrategy,obdHourTransfor);
            IntegratedDealTransforInterface integratedDealTransfor = new HourIntegratedCalcBiz();
            Map<String,String>  integrityMap = integratedDealTransfor.convertData2Map(hourIntegratedTransfor);

            System.out.println("args = [" + integrityMap + "]");
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private static Map<String,String[]> createFamilyQualifiers() {
        Map<String,String[]> familyQualifiers = new HashMap<>();
        familyQualifiers.put(HBaseTable.HOUR_STATISTICS.getFirstFamilyName(),HbaseColumn.HourStatisticsCloumn.obdHourColumns);
        familyQualifiers.put(HBaseTable.HOUR_STATISTICS.getFourthFamilyName(),HbaseColumn.HourStatisticsCloumn.deHourColumns);
        return familyQualifiers;
    }

    private static   <K extends CSTData,N extends CSTData> K findHbase(Map map, IHBaseQueryAndPersistStrategy<K> ihBaseQueryAndPersistStrategy, N needData){

        CstConstants.TIME_SELECT timeSelect= CstConstants.TIME_SELECT.HOUR;
        TimeSelectRowKeyGrenerate timeSelectRowKeyGrenerate = new TimeSelectRowKeyGrenerate(needData.getCarId(),
                needData.getTime(),
                timeSelect);
        map.put(OtherKey.DataDealKey.ROWKEY_GENERATE, timeSelectRowKeyGrenerate);
        return ihBaseQueryAndPersistStrategy.findHBaseData(map);
    }
    private static Charset  getCharset(String encoding) {
        return (StringUtils.hasText(encoding) ? Charset.forName(encoding) : Charset.forName("UTF-8"));
    }
}

package cst.jstorm.hour.bolt.gps;

import com.alibaba.fastjson.JSON;
import com.cst.stream.common.CstConstants;
import com.cst.stream.common.HBaseTable;
import com.cst.stream.common.HbaseColumn;
import com.cst.stream.stathour.gps.GpsHourTransfor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.cst.jstorm.commons.stream.constants.OtherKey;
import com.cst.jstorm.commons.stream.constants.PropKey;
import com.cst.jstorm.commons.stream.operations.GeneralStreamHttp;
import com.cst.jstorm.commons.stream.operations.HBaseOperation;
import com.cst.jstorm.commons.stream.operations.hbasestrategy.IHBaseQueryAndPersistStrategy;
import com.cst.jstorm.commons.stream.operations.hbasestrategy.StrategyChoose;
import com.cst.jstorm.commons.utils.HttpURIUtil;
import com.cst.jstorm.commons.utils.PropertiesUtil;
import com.cst.jstorm.commons.utils.http.HttpUtils;
import com.cst.jstorm.commons.utils.spring.MyApplicationContext;
import org.apache.hadoop.hbase.client.Connection;
import org.springframework.context.support.AbstractApplicationContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author Johnney.Chiu
 * create on 2018/1/31 16:27
 * @Description
 * @title
 */
public class Test2 {
    @SuppressWarnings("unchecked")
    public static void main(String... args) throws JsonProcessingException {
        AbstractApplicationContext context = null;

        GpsHourTransfor gpsHourTransfor = new GpsHourTransfor("72b41560207a46ac9dc6e647c38ff086", 1514889673000l, 22,  0
                , 1);
        String msg = JSON.toJSONString(gpsHourTransfor);
        System.out.println(msg);
        try {
            context= MyApplicationContext.getDefaultContext();
            HttpUtils httpUtils = (HttpUtils) context.getBean("httpUtils");

            Connection connection = (Connection) context.getBean(OtherKey.DataDealKey.HBASE_CONNECTION);

            Map supplementMap = new HashMap<String, Object>() {{
                put(OtherKey.DataDealKey.TIME_SELECT, CstConstants.TIME_SELECT.HOUR);
            }};
            Properties prop = null;
            prop = PropertiesUtil.initProp(prop, false);
            GeneralStreamHttp generalStreamHttp = new GeneralStreamHttp(httpUtils, prop.getProperty("url_base"), HttpURIUtil.GPS_HOUR_SAVE);
            HBaseOperation<GpsHourTransfor> hBaseOperation = new HBaseOperation(connection, HBaseTable.HOUR_STATISTICS.getTableName(),
                    HBaseTable.HOUR_STATISTICS.getFirstFamilyName(), HbaseColumn.HourStatisticsCloumn.gpsHourColumns,
                    GpsHourTransfor.class);

            IHBaseQueryAndPersistStrategy<GpsHourTransfor> ihBaseQueryAndPersistStrategy = StrategyChoose.
                    generateStrategy((String) prop.get(PropKey.DEAL_STRATEGY), generalStreamHttp, hBaseOperation);
            ihBaseQueryAndPersistStrategy.persistHBaseData(supplementMap, msg);
        }
        catch (Exception e){
            e.printStackTrace();
        }
        finally {

            context.close();
        }
    }
}

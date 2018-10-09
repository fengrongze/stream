package com.cst.jstorm.commons.stream.operations.hbasestrategy;

import com.cst.stream.stathour.CSTData;
import com.cst.jstorm.commons.stream.constants.OtherKey;
import com.cst.jstorm.commons.stream.operations.GeneralStreamHttp;
import com.cst.jstorm.commons.stream.operations.HBaseOperation;
import com.cst.jstorm.commons.utils.http.HttpUtils;
import org.apache.hadoop.hbase.client.Connection;

import java.util.Map;

/**
 * @author Johnney.Chiu
 * create on 2018/1/30 17:06
 * @Description
 * @title
 */
public class StrategyChoose{

    public static <T extends CSTData>  IHBaseQueryAndPersistStrategy<T>  generateStrategy (String strategy,GeneralStreamHttp generalStreamHttp,
                                                   HBaseOperation<T> hBaseOperation){
        if(strategy.contains(OtherKey.DataDealKey.DEAL_STRATEGY_HTTP))
            return new HttpGeneralStreamExecution<>(generalStreamHttp);
        return new HBaseGeneralStreamExecution<>(hBaseOperation);
    }
    @SuppressWarnings("unchecked")
    public static <T extends CSTData>  IHBaseQueryAndPersistStrategy<T>  generateStrategy(String strategy,
                                                                                          HttpUtils httpUtils,
                                                                                          String url_base,
                                                                                          String data_save,
                                                                                          Connection connection,
                                                                                          String tableName,
                                                                                          String familyName,
                                                                                          String[] columns,
                                                                                          Class<T> clazz){
        if(strategy.contains(OtherKey.DataDealKey.DEAL_STRATEGY_HTTP))
            return new HttpGeneralStreamExecution<>(new GeneralStreamHttp(httpUtils, url_base, data_save));
        return new HBaseGeneralStreamExecution<>(new HBaseOperation(connection, tableName,
                    familyName, columns,
                    clazz));
    }


    public static <T extends CSTData>  IHBaseQueryAndPersistStrategy<T>  generateStrategy(
                                                                                          Connection connection,
                                                                                          String tableName,
                                                                                          String familyName,
                                                                                          String[] columns,
                                                                                          Class<T> clazz){
        return new HBaseGeneralStreamExecution<>(new HBaseOperation(connection, tableName,
                familyName, columns,
                clazz));
    }

    public static <T extends CSTData>  IHBaseQueryAndPersistStrategy<T>  generateStrategy(
            Connection connection,
            String tableName,
            Map<String,String[]> familyQualifiers,
            Class<T> clazz){
        return new HBaseGeneralStreamExecution<>(new HBaseOperation( connection,  tableName,  familyQualifiers, clazz));
    }
}

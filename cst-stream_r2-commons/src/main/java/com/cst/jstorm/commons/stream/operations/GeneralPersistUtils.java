package com.cst.jstorm.commons.stream.operations;

import com.cst.jstorm.commons.stream.constants.OtherKey;
import com.cst.jstorm.commons.stream.operations.hbasestrategy.IHBaseQueryAndPersistStrategy;
import com.cst.stream.common.CstConstants;
import com.cst.stream.common.JsonHelper;
import com.cst.stream.stathour.CSTData;
import com.cst.stream.stathour.LastTimeData;
import com.fasterxml.jackson.core.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Johnney.Chiu
 * create on 2018/5/9 17:18
 * @Description persist utils
 * @title
 */
public class GeneralPersistUtils {

    private final static Logger logger = LoggerFactory.getLogger(GeneralPersistUtils.class);

    public static  <T extends CSTData> void dataPersist(String msg, IHBaseQueryAndPersistStrategy<T> ihBaseQueryAndPersistStrategy,
                                                           CstConstants.TIME_SELECT time_select) {
        try {
            logger.debug(" persist data:{}", msg);
            Map supplementMap = new HashMap<String, Object>() {{
                put(OtherKey.DataDealKey.TIME_SELECT, time_select);
            }};
            ihBaseQueryAndPersistStrategy.persistHBaseData(supplementMap, msg);
        } catch (Exception e) {
            logger.error(" persist bolt execute error:{}", e);
        }
    }


    public static <T extends CSTData>  void noDelayDataPersist(String msg, IHBaseQueryAndPersistStrategy<T> ihBaseQueryAndPersistStrategy,final String type){
        try {
            logger.debug(" persist no delay persist data:{}", msg);
            LastTimeData<T> lastTimeData = JsonHelper.toBeanWithoutException(msg, new TypeReference<LastTimeData<T>>(){
            });
            ihBaseQueryAndPersistStrategy.persistHBaseData(new HashMap<String, String>(){{put(OtherKey.MIDLLE_DEAL.LAST_DATA_PARAM, type);}}, lastTimeData.getData());
        } catch (Exception e) {
            logger.error("persist no delay hour error:{},error data is {}",e,msg);
        }
    }

    public static <T extends CSTData>  void firstDataPersist(String msg, IHBaseQueryAndPersistStrategy<T> ihBaseQueryAndPersistStrategy,
                                                               final String type,final CstConstants.TIME_SELECT time_select){
        try {
            logger.debug(" persist first data:{}", msg);
            T firstData = JsonHelper.toBeanWithoutException(msg, new TypeReference<T>(){
            });
            ihBaseQueryAndPersistStrategy.persistHBaseData(new HashMap<String, Object>(){{
                put(OtherKey.DataDealKey.TIME_SELECT, time_select);
                put(OtherKey.MIDLLE_DEAL.BESINESS_KEY_TYPE, type);
            }}, firstData);
        } catch (Exception e) {
            logger.error("persist first data is {}",msg,e);
        }
    }

    public static  <T extends CSTData> void resultPersist(String msg, IHBaseQueryAndPersistStrategy<T> ihBaseQueryAndPersistStrategy,
                                                        final String type,final CstConstants.TIME_SELECT time_select) {
        try {
            logger.debug(" persist data:{}", msg);
            Map supplementMap = new HashMap<String, Object>() {{
                put(OtherKey.DataDealKey.TIME_SELECT, time_select);
            }};
            ihBaseQueryAndPersistStrategy.persistHBaseData(supplementMap, msg);
        } catch (Exception e) {
            logger.error(" persist data error:{}", msg,e);
        }
    }


}

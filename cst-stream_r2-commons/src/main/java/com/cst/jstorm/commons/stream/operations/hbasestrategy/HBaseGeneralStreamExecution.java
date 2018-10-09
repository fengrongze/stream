package com.cst.jstorm.commons.stream.operations.hbasestrategy;

import com.cst.stream.common.CstConstants;
import com.cst.stream.common.JsonHelper;
import com.cst.stream.stathour.CSTData;
import com.fasterxml.jackson.core.type.TypeReference;
import com.cst.jstorm.commons.stream.constants.OtherKey;
import com.cst.jstorm.commons.stream.operations.HBaseOperation;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.cst.jstorm.commons.stream.constants.OtherKey.MIDLLE_DEAL.LAST_DATA_PARAM;


/**
 * @author Johnney.chiu
 * create on 2018/1/25 16:17
 * @Description
 */
public class HBaseGeneralStreamExecution< T extends CSTData>
        implements IHBaseStrategyWithOwn<T> {
    private final static Logger logger = LoggerFactory.getLogger(HBaseGeneralStreamExecution.class);

    private HBaseOperation<T> hBaseOperation;

    public HBaseGeneralStreamExecution(HBaseOperation<T> hBaseOperation) {
        this.hBaseOperation = hBaseOperation;
    }

    public HBaseGeneralStreamExecution() {
    }
    @SuppressWarnings("unchecked")
    @Override
    public T findHBaseData(Map map) {

        IRowKeyGrenate<T> rowKeyGrenate = (IRowKeyGrenate<T>) map.get(OtherKey.DataDealKey.ROWKEY_GENERATE);
        hBaseOperation.createiRowKeyGrenate(rowKeyGrenate).createRowKey();
        logger.debug("####find key is {}", hBaseOperation.getRowKey());
        return hBaseOperation.getCstData();

    }

    @Override
    public boolean persistHBaseData(Map map, T t) {

        if (StringUtils.isEmpty(hBaseOperation.getRowKey())) {
            if (null != map.get(OtherKey.DataDealKey.TIME_SELECT) && null != map.get(OtherKey.MIDLLE_DEAL.BESINESS_KEY_TYPE)) {
                hBaseOperation.createiRowKeyGrenate(new TimeSelectAndTypeRowKeyGrenerate<T>((CstConstants.TIME_SELECT) map.get(OtherKey.DataDealKey.TIME_SELECT), t,
                        (String) map.get(OtherKey.MIDLLE_DEAL.BESINESS_KEY_TYPE))).createRowKey();
                logger.debug("####TimeSelectAndTypeRowKeyGrenerate persist data is {},carId:{},time:{}",t.toString(),t.getCarId(),t.getTime());
            } else if (null != map.get(OtherKey.DataDealKey.TIME_SELECT)) {
                hBaseOperation.createiRowKeyGrenate(new TimeSelectRowKeyGrenerate<>(
                        (CstConstants.TIME_SELECT) map.get(OtherKey.DataDealKey.TIME_SELECT), t)).createRowKey();
                logger.debug("####TimeSelectRowKeyGrenerate persist data is {},carId:{},time:{}",t.toString(),t.getCarId(),t.getTime());

            } else {
                hBaseOperation.createiRowKeyGrenate(new NoDelayRowKeyGenerate<>(t, (String) map.get(LAST_DATA_PARAM))).createRowKey();
            }
        }

        return hBaseOperation.putCstData(t);
    }
    @Override
    public boolean persistHBaseData(Map map, String source) {
        try {
            T t = JsonHelper.toBeanWithoutException(source, new TypeReference<T>() {
            });
            return persistHBaseData(map, t);
        } catch (Exception e) {
            logger.error("parse data {} error", source);
            e.printStackTrace();
        }
        return false;
    }


    @Override
    public T findHBaseData(Connection connection,String tableName,String familyName,
                           String rowKey,Class<T> clazz,String... columns) {
        T t = null;
        try {
            return new HBaseOperation<>(connection, tableName, familyName, columns, clazz).createRowKey(rowKey)
                    .getCstData();

        }catch (Exception e){
            logger.error("find data {} is error:{}",t,e);
        }
        return t;

    }


    @Override
    public boolean persistHBaseData(Connection connection,String tableName,String familyName,
                                    String rowKey,T t){
        if (t == null) {
            return false;
        }

        try{
            return new HBaseOperation<>(connection, tableName,familyName,t).createRowKey(rowKey)
            .putCstData(familyName,rowKey,t);
        }catch (Exception e){
            logger.error("put data {} with client error,{}", t.toString(), e.getMessage());
            e.printStackTrace();
        }
        return false;
    }
    @Override
    public boolean persistHBaseData(Connection connection,String tableName,String familyName,
                                    String rowKey,String source) {
        if (StringUtils.isEmpty(source)) {
            return false;
        }
        try {
            T t = JsonHelper.toBeanWithoutException(source, new TypeReference<T>() {
            });
            return persistHBaseData(connection, tableName, familyName, rowKey, t);
        } catch (Exception e) {
            e.printStackTrace();

        }
        return false;
    }
}

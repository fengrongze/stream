package com.cst.jstorm.commons.stream.operations.hbasestrategy;

import com.alibaba.dubbo.common.utils.StringUtils;
import com.cst.stream.common.JsonHelper;
import com.cst.stream.stathour.CSTData;
import com.cst.stream.stathour.CstStreamBaseResult;
import com.fasterxml.jackson.core.type.TypeReference;
import com.cst.jstorm.commons.stream.constants.OtherKey;
import com.cst.jstorm.commons.stream.operations.GeneralStreamHttp;
import com.cst.jstorm.commons.utils.http.HttpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author Johnney.chiu
 * create on 2018/1/23 11:13
 * @Description 实现Http方式的数据持久或者查询
 */
public class HttpGeneralStreamExecution<T extends CSTData>
        implements IHBaseStrategyWithHttp<T> {
    private final static Logger logger = LoggerFactory.getLogger(HttpGeneralStreamExecution.class);

    private GeneralStreamHttp generalStreamHttp;


    public HttpGeneralStreamExecution( GeneralStreamHttp generalStreamHttp) {
        this.generalStreamHttp = generalStreamHttp;
    }

    public HttpGeneralStreamExecution() {
    }


    @Override
    public T findHBaseData(HttpUtils httpUtils, String url) {
        T t = null;
        try {
            String temp = GeneralStreamHttp.getGeneralData(httpUtils, url);
            if (temp == null)
                return null;
            CstStreamBaseResult<T> result = JsonHelper.toBeanWithoutException(temp, new TypeReference<CstStreamBaseResult<T>>() {
            });
            if (result == null)
                return null;
            if (result.getData() == null)
                return null;
            t = result.getData();
        } catch (Exception e) {
            logger.error("io exception {}",e);
        }
        return t;
    }

    @Override
    public boolean persistHBaseData(HttpUtils httpUtils, String msg, String url) {
        if (!GeneralStreamHttp.putHttpData(httpUtils, msg, url)) {
            logger.error("this data can not persist:{}", msg);
            return false;
        }
        return true;
    }

    @Override
    public T findHBaseData(Map map) {
        if(generalStreamHttp.getContextUrl().contains("%s"));
            generalStreamHttp.createUrl((String[]) map.get(OtherKey.DataDealKey.PARAMS));
        return findHBaseData();
    }

    public T findHBaseData(){
        T t = null;
        String msg= generalStreamHttp.getGeneralData();
        if(StringUtils.isEmpty(msg))
            return t;
        CstStreamBaseResult<T> result = null;
        try {
            result = JsonHelper.toBeanWithoutException(msg, new TypeReference<CstStreamBaseResult<T>>() {
            });
            t = result.getData();
        } catch (Exception e) {
            logger.error("parse data {} error:{}",t,e);
        }
        return t;
    }
    @Override
    public boolean persistHBaseData(Map map, T t) {
        if(t==null)
            return false;
        return persistHBaseData(map, JsonHelper.toStringWithoutException(t));
    }

    @Override
    public boolean persistHBaseData(Map map, String msg) {

        return generalStreamHttp.putHttpData(msg);

    }
}

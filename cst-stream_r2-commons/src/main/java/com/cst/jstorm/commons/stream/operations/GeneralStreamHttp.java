package com.cst.jstorm.commons.stream.operations;

import com.cst.jstorm.commons.utils.http.HttpResult;
import com.cst.jstorm.commons.utils.http.HttpUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;

/**
 * @author Johnney.chiu
 * create on 2017/12/5 14:37
 * @Description 持久化
 */
public class GeneralStreamHttp {
    private final static Logger logger = LoggerFactory.getLogger(GeneralStreamHttp.class);

    private HttpUtils httpUtils;

    private String baseUrl;

    private String detailUrl;

    private String contextUrl;

    private String msg;

    public GeneralStreamHttp() {
    }

    public GeneralStreamHttp(HttpUtils httpUtils, String baseUrl, String detailUrl, String contextUrl, String msg) {
        this.httpUtils = httpUtils;
        this.baseUrl = baseUrl;
        this.detailUrl = detailUrl;
        this.contextUrl = contextUrl;
        this.msg = msg;
    }

    public GeneralStreamHttp(HttpUtils httpUtils, String baseUrl, String detailUrl) {
        this.httpUtils = httpUtils;
        this.baseUrl = baseUrl;
        this.detailUrl = detailUrl;
    }

    public boolean postHttpData(){
        return postHttpData(httpUtils, msg, contextUrl);
    }


    //数据持久化
    public static boolean postHttpData(HttpUtils httpUtils, String msg, String url ){
        try {
            HttpResult httpResult=httpUtils.postjson(url, msg,
                    new HashMap<String, String>() {{
                        put("Content-Type", "application/json");
                    }});
            if (httpResult!=null && httpResult.getStatus() == 200) {
                logger.debug("post data :{} successfully!",msg);
                return true;
            }else{
                logger.debug("post data :{} error!",msg);
                return false;
            }

        } catch (IOException e) {
            logger.error("data is error:{}",e);
        }
        return false;
    }

    public String getGeneralData() {
        return getGeneralData(httpUtils, contextUrl);
    }
    public static String getGeneralData(HttpUtils httpUtils,String url){
        HttpResult resultData=httpUtils.get(url,
                new HashMap<String, String>() {{
                    put("Content-Type", "application/json");
                }});
        if(resultData!=null &&resultData.getStatus()==200&& StringUtils.isNotEmpty(resultData.getData())){
            return resultData.getData();
        }
        return null;
    }

    public boolean putHttpData() {
        return putHttpData(httpUtils, msg, contextUrl);
    }
    public boolean putHttpData(String msg) {
        return postHttpData(httpUtils, msg, contextUrl);
    }
    //数据持久化
    public static boolean putHttpData(HttpUtils httpUtils,String msg,String url){
        try {
            HttpResult httpResult=httpUtils.putjson(url, msg,
                    new HashMap<String, String>() {{
                        put("Content-Type", "application/json");
                    }});
            if (httpResult!=null && httpResult.getStatus() == 200) {
                logger.debug("put data :{} successfully!",msg);
                return true;
            }else{
                logger.error("put data :{} error!",msg);
                return false;
            }

        } catch (IOException e) {
            e.printStackTrace();
            logger.error("put data error:",e);
        }
        return false;
    }


    public static String generateUrl(String url,String... params) {
        return  String.format(url, params);
    }

    public GeneralStreamHttp createUrl(String... params){
         contextUrl=generateUrl(baseUrl.concat(detailUrl), params);
        return this;
    }

    public GeneralStreamHttp createMsg(String msg){
        this.msg = msg;
        return this;
    }

    public HttpUtils getHttpUtils() {
        return httpUtils;
    }

    public void setHttpUtils(HttpUtils httpUtils) {
        this.httpUtils = httpUtils;
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public String getDetailUrl() {
        return detailUrl;
    }

    public void setDetailUrl(String detailUrl) {
        this.detailUrl = detailUrl;
    }

    public String getContextUrl() {
        return contextUrl;
    }

    public void setContextUrl(String contextUrl) {
        this.contextUrl = contextUrl;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }
}

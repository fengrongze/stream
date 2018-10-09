package com.cst.jstorm.commons.utils.http;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;

/**
 * @author Johnney.chiu
 * create on 2017/11/30 14:06
 * @Description 基本的http访问
 */
public class HttpUtils {
    private static Logger logger = LoggerFactory.getLogger(HttpUtils.class);
    private CloseableHttpClient httpClient;
    private RequestConfig requestConfig;

    public HttpUtils(CloseableHttpClient httpClient,RequestConfig requestConfig){
        this.httpClient = httpClient;
        this.requestConfig = requestConfig;
    }

    public HttpResult postjson(String url, String json, Map<String, String> headers)throws  IOException {
        HttpPost httpPost = new HttpPost(url);
        if(headers !=null && !headers.isEmpty()){
            for(Map.Entry<String,String> entry:headers.entrySet())
                httpPost.setHeader(entry.getKey(),entry.getValue());
        }

        StringEntity entity = new StringEntity(json, Charset.forName("UTF-8"));
        httpPost.setEntity(entity);

        httpPost.setConfig(this.requestConfig);

        CloseableHttpResponse response = null;
        // 执行请求
        try {
            response = this.httpClient.execute(httpPost);
            return new HttpResult(response.getStatusLine().getStatusCode(),
                    EntityUtils.toString(response.getEntity(), "UTF-8"));
        }catch (Exception e) {
            logger.error("error message:{}\n,post {} 请求失败 data is {}", e.getMessage(),url,json);
            try {
                if(response!=null && response.getEntity()!=null && response.getEntity().getContent()!=null){
                    response.getEntity().getContent().close();
                }
            } catch (IOException e1) {

            }

        }
        return null;
    }
    public HttpResult putjson(String url, String json, Map<String, String> headers)throws  IOException {
        HttpPut httpPut = new HttpPut(url);
        if(headers !=null && !headers.isEmpty()){
            for(Map.Entry<String,String> entry:headers.entrySet())
                httpPut.setHeader(entry.getKey(),entry.getValue());
        }

        StringEntity entity = new StringEntity(json, Charset.forName("UTF-8"));
        httpPut.setEntity(entity);

        httpPut.setConfig(this.requestConfig);

        CloseableHttpResponse response = null;
        // 执行请求
        try {
            response = httpClient.execute(httpPut);
            return new HttpResult(response.getStatusLine().getStatusCode(),
                    EntityUtils.toString(response.getEntity(), "UTF-8"));
        }catch (Exception e) {
            logger.error("error message:{}\n,put {}请求失败 data is {}", e.getMessage(),url,json);
            try {
                if(response!=null && response.getEntity()!=null && response.getEntity().getContent()!=null){
                    response.getEntity().getContent().close();
                }
            } catch (IOException e1) {

            }
        }
        return null;
    }
    public HttpResult get(String url, Map<String, String> headers){
        HttpGet httpGet = new HttpGet(url);
        CloseableHttpResponse response = null;
        try{
            if(headers !=null && !headers.isEmpty()){
                for(Map.Entry<String,String> entry:headers.entrySet())
                    httpGet.setHeader(entry.getKey(),entry.getValue());
            }

            response = httpClient.execute(httpGet);
            return new HttpResult(response.getStatusLine().getStatusCode(),
                    EntityUtils.toString(response.getEntity(), "UTF-8"));
        } catch (Exception e) {
            logger.error("error message:{}\n,get {}请求失败", e.getMessage(),url);
            try {
                if(response!=null && response.getEntity()!=null && response.getEntity().getContent()!=null){
                    response.getEntity().getContent().close();
                }
            } catch (IOException e1) {

            }
        }
        return null;
    }

}

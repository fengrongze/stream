package com.cst.jstorm.commons.stream.custom;

import com.cst.jstorm.commons.stream.operations.GeneralStreamHttp;
import com.cst.jstorm.commons.utils.CryptogramUtil;
import com.cst.jstorm.commons.utils.http.HttpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

/**
 * @author Johnney.chiu
 * create on 2017/12/8 11:51
 * @Description 百度api调用
 */
public class BaiduApiAccess {

    final static Logger logger = LoggerFactory.getLogger(BaiduApiAccess.class);

    //la,lo,ak,sk
    public static final String SN_ORIGINAL = "/geocoder/v2/?location=%s,%s&output=json&ak=%s%s";

    //la,lo,ak,sn
    public static final String LOCATION_URL = "http://api.map.baidu.com/geocoder/v2/?location=%s,%s&output=json&ak=%s&sn=%s";

    public static final String DEFUALT_CHARSET = "UTF-8";

    public static String getProvinceWithBaiduApiSN(HttpUtils httpUtils, String sn, String ak, String la, String lo){
        String url = String.format(LOCATION_URL, la, lo, ak, sn);
        return GeneralStreamHttp.getGeneralData(httpUtils, url);
    }
    public static String getProvinceWithBaiduApiSN(String biduLocationUrl,HttpUtils httpUtils,String sn, String ak,String la, String lo){
        String url = String.format(biduLocationUrl, la, lo, ak, sn);
        return GeneralStreamHttp.getGeneralData(httpUtils, url);
    }

    public static String generateSN(String la,String lo,String ak,String sk)  {
        String snStr = String.format(SN_ORIGINAL, la, lo, ak, sk);
        try {
            snStr= URLEncoder.encode(snStr, DEFUALT_CHARSET);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return CryptogramUtil.md5Encrypt(snStr);
    }
    public static String generateSN(String snOriginal,String la,String lo,String ak,String sk)  {
        String snStr = String.format(snOriginal, la, lo, ak, sk);
        try {
            snStr= URLEncoder.encode(snStr, DEFUALT_CHARSET);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return CryptogramUtil.md5Encrypt(snStr);
    }



}

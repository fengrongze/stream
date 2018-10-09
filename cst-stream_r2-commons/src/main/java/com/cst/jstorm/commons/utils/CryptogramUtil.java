package com.cst.jstorm.commons.utils;

import org.apache.commons.lang.StringUtils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * @author Johnney.chiu
 * create on 2017/12/8 10:25
 * @Description 加密解密
 */
public class CryptogramUtil {

    private static final String MD5 = "MD5";

    public static String md5Encrypt(String str){
        if(StringUtils.isEmpty(str))
            return "";
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance(MD5);
            byte[] array = md.digest(str.getBytes());
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < array.length; ++i) {
                sb.append(Integer.toHexString((array[i] & 0xFF) | 0x100).substring(1, 3));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return "";
    }
}

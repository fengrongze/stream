package com.cst.bigdata.utils;

import com.cst.bigdata.domain.mybatis.AppOilPrice;
import com.cst.stream.stathour.CSTData;
import com.cst.stream.stathour.CstStreamBaseResult;
import com.cst.stream.stathour.am.AmHourTransfor;
import com.cst.stream.stathour.de.DeHourTransfor;
import com.cst.stream.vo.AppOilVo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.springframework.beans.BeanUtils;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Johnney.chiu
 * create on 2017/12/12 17:01
 * @Description
 */
public class Test {

    public class A{}

    public class B{}

    public class C{}


    public static void main(String... args) throws ParseException {
        DeHourTransfor deHourTransfor =BeanUtils.instantiate(DeHourTransfor.class);

        AmHourTransfor amHourTransfor = new AmHourTransfor();
        CstStreamBaseResult<CSTData> baseResult = CstStreamBaseResult.success(amHourTransfor);
        CstStreamBaseResult<DeHourTransfor> baseResult1 = CstStreamBaseResult.success(deHourTransfor);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            System.out.println(objectMapper.writeValueAsString(baseResult));
            System.out.println(objectMapper.writeValueAsString(baseResult));
            System.out.println(objectMapper.writeValueAsString(deHourTransfor));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }



        //System.out.println(deHourTransfor.getCarId());

/*
        String str = "0000139ca0124edb90cb5a5d27ab2b43";
        int mod = Math.abs(str.hashCode()) ;
        SimpleDateFormat myFmt = new SimpleDateFormat("yyyyMMdd");
        Date date=myFmt.parse("20171219");
        String a=mod+""+(date.getTime());
        byte[] b = a.getBytes();

        System.out.println(String.format("mod is %d str is %s and bytes length is %d",mod, a,b.length));


        String str2 = MD5Hash.getMD5AsHex(Bytes.toBytes(str));
        String str3=str2+(date.getTime());
        b = str3.getBytes();
        System.out.println(String.format("mod is %s str is %s and bytes length is %d",str2, str3,b.length));
*/
    }


    public void testStram(){

        List<AppOilPrice> appOilPrices = new ArrayList<>();
        AppOilPrice appOilPrice = new AppOilPrice();
        appOilPrice.setB0(122D);
        appOilPrice.setProvince("xiaoq");
        appOilPrices.add(appOilPrice);
        appOilPrice = new AppOilPrice();
        appOilPrice.setB0(123D);
        appOilPrice.setProvince("xiaoq");
        appOilPrices.add(appOilPrice);

        List<AppOilVo> appOilVos = appOilPrices.stream()
                .map(a -> {
                    AppOilVo appOilVo = new AppOilVo();
                    BeanUtils.copyProperties(a, appOilVo);
                    return appOilVo;
                }).collect(Collectors.toList());
        appOilVos.stream().map(a->a.getProvince()).forEach(System.out::println);

        appOilVos = appOilPrices.stream()
                .collect(ArrayList::new, (list, item) -> {
                    AppOilVo appOilVo = new AppOilVo();
                    BeanUtils.copyProperties(item, appOilVo);
                    list.add(appOilVo);
                }, List::addAll);
        appOilVos.stream().map(a->a.getB0()).forEach(System.out::println);
    }


}




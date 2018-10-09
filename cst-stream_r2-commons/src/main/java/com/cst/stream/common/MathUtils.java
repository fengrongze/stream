package com.cst.stream.common;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * @author Johnney.Chiu
 * create on 2018/7/24 15:59
 * @Description 计算数据工具类
 * @title
 */
public class MathUtils {

    public static final double round(BigDecimal bigDecimal, int scale) {
        if(scale<0)
            return bigDecimal.doubleValue();
        return bigDecimal.setScale(scale, RoundingMode.HALF_UP).doubleValue();
    }

    public static final double divideTransfor( double dividend, double divisor,int scale) {
        if (divisor == 0D) {
            return 0;
        }
        return round(new BigDecimal(dividend).divide(new BigDecimal(divisor), scale, RoundingMode.HALF_UP),scale);
    }

    public static final double divideTransfor(int dividend, long divisor,int scale) {
        if (divisor == 0D) {
            return 0;
        }
        return round(new BigDecimal(dividend).divide(new BigDecimal(divisor), scale, RoundingMode.HALF_UP),scale);
    }

    public static final double divideTransfor(float dividend, long divisor,int scale) {
        if (divisor == 0D) {
            return 0;
        }
        return round(new BigDecimal(dividend).divide(new BigDecimal(divisor), scale, RoundingMode.HALF_UP),scale);
    }

    public static void main(String... args){
       System.out.println( MathUtils.round(BigDecimal.valueOf(14050.89F).subtract(BigDecimal.valueOf(14050.889F)),3));
       System.out.println(Float.MAX_VALUE);
        System.out.println(Integer.MAX_VALUE);
    }
}

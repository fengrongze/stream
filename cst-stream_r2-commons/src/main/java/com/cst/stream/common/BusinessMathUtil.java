package com.cst.stream.common;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.math.NumberUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;

import static com.cst.stream.common.MathUtils.round;

/**
 * @author Johnney.Chiu
 * create on 2018/6/28 20:54
 * @Description
 * @title
 */
@Slf4j
public class BusinessMathUtil {



    public static final float calcFuelPerHundred(Float fuel,Float mileage){
        if (fuel == null || mileage == null||mileage==0) {
            return 0F;
        }
        return (float) MathUtils.round(new BigDecimal(fuel).divide(new BigDecimal(mileage),3, RoundingMode.HALF_UP).multiply(new BigDecimal(100)), 3);
    }

    public static final double calcFuelPerHundred(Double fuel,Double mileage){
        if (fuel == null || mileage == null||mileage==0) {
            return 0D;
        }
        return  MathUtils.round(new BigDecimal(fuel).divide(new BigDecimal(mileage),3, RoundingMode.HALF_UP).multiply(new BigDecimal(100)), 3);
    }

    public static final float calcAvarageSpeed(Float mileage,Integer duration){
        if (duration == null || mileage == null||duration==0) {
            return 0F;
        }
        return (float) MathUtils.round( new BigDecimal(mileage).divide(new BigDecimal(duration),10, RoundingMode.HALF_UP).multiply(new BigDecimal(3600)), 3);
    }

    public static final double calcAvarageSpeed(Double mileage,Integer duration){
        if (duration == null || mileage == null||duration==0) {
            return 0D;
        }
        return  MathUtils.round( new BigDecimal(mileage).divide(new BigDecimal(duration),10, RoundingMode.HALF_UP).multiply(new BigDecimal(3600)), 3);
    }


    public static final  float clacFee(Float oldFuel,Float newFuel,Map<String,Object> map){
        if(newFuel<oldFuel)
            return 0f;
        try {
            BigDecimal total = BigDecimal.valueOf(newFuel);
            BigDecimal oldTotal = BigDecimal.valueOf(oldFuel);
            if(null!=map.get("fuelPrice")){
                if(NumberUtils.toFloat(map.get("fuelPrice").toString())<=0){
                    map.put("fuelPrice","7.8");
                }
            }
            else{
                map.put("fuelPrice","7.8");
            }
            return (float) round((total.subtract(oldTotal)).multiply(BigDecimal.valueOf(NumberUtils.toFloat((String) map.get("fuelPrice"),7.8F))), 3);
        }catch (Exception e){
            log.error("fuelPrice is {}",map.get("fuelPrice"));
        }
        return 0f;
    }

    public static final  double clacFee(Double oldFuel,Double newFuel,Map<String,Object> map){
        if(newFuel<oldFuel)
            return 0f;
        try {
            BigDecimal total = BigDecimal.valueOf(newFuel);
            BigDecimal oldTotal = BigDecimal.valueOf(oldFuel);
            if(null!=map.get("fuelPrice")){
                if(NumberUtils.toFloat(map.get("fuelPrice").toString())<=0){
                    map.put("fuelPrice","7.8");
                }
            }
            else{
                map.put("fuelPrice","7.8");
            }
            return round((total.subtract(oldTotal)).multiply(BigDecimal.valueOf(NumberUtils.toFloat((String) map.get("fuelPrice"),7.8F))), 3);
        }catch (Exception e){
            log.error("fuelPrice is {}",map.get("fuelPrice"));
        }
        return 0D;
    }

}

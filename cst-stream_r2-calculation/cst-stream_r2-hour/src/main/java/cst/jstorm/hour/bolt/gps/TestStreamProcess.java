package cst.jstorm.hour.bolt.gps;

import com.cst.stream.common.DateTimeUtil;
import com.cst.stream.common.MathUtils;
import com.cst.stream.common.StreamRedisConstants;
import com.cst.stream.stathour.voltage.VoltageHourSource;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * @author Johnney.Chiu
 * create on 2018/3/29 11:10
 * @Description Test
 * @title
 */
public class TestStreamProcess {
    public static void main(String... args) throws IOException {
        String str = DateTimeUtil.toLongTimeString(1543619722000L, DateTimeUtil.DEFAULT_DATE_DEFULT);
        int hour=Integer.parseInt(str.substring(11,13));
        boolean isNight = false;
        if(hour>=23||hour<6){
            isNight = true;
        }
        System.out.println(isNight);
    }

    public static float calcFuelPerHundred(Float fuel,Float mileage){
        if (fuel == null || mileage == null) {
            return 0F;
        }
        return (float) MathUtils.round(new BigDecimal(fuel).divide(new BigDecimal(mileage),3, RoundingMode.HALF_UP).multiply(new BigDecimal(100)), 3);
    }
}

package cst.jstorm.hour.test;

import com.cst.stream.common.BusinessMathUtil;
import com.cst.stream.common.MathUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class AvarageSpeedTest {

    public static void main(String[] args) {
        //（11.644/280）*3600 = 149.71km/h
        float mileage = 11.644F;
        int duration = 280;
        float as = BusinessMathUtil.calcAvarageSpeed(11.644F,280);
        BigDecimal bd = new BigDecimal(mileage).divide(new BigDecimal(duration),10, RoundingMode.HALF_UP).multiply(new BigDecimal(3600));
        //new BigDecimal(mileage).divide(new BigDecimal(duration).multiply(new BigDecimal(3600)),3, RoundingMode.HALF_UP);
        System.out.println("平均速度 = [" + MathUtils.round(bd,3) + "]");
    }
}

package cst.jstorm.daymonth.test;

import com.cst.stream.common.JsonHelper;
import com.cst.stream.stathour.obd.ObdHourSource;
import com.fasterxml.jackson.core.type.TypeReference;

public class JsonHelperTest {
    public static void main(String[] args) {
        ObdHourSource obdHourSource= JsonHelper.toBeanWithoutException(null, new TypeReference<ObdHourSource>() {
        });

        System.out.println("args = [" + obdHourSource + "]");
    }
}

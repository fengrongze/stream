package cst.jstorm.hour.bolt.gps;

import com.cst.stream.stathour.obd.ObdHourLatestData;
import com.cst.stream.stathour.obd.ObdHourSource;
import com.cst.jstorm.commons.stream.constants.ExceptionCodeStatus;
import cst.jstorm.hour.calcalations.obd.ObdHourCalcBiz;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Johnney.Chiu
 * create on 2018/7/16 11:44
 * @Description
 * @title
 */
public class Test6 {

    public static void main(String...args){
        ObdHourCalcBiz obdHourCalcBiz = new ObdHourCalcBiz();
        ObdHourSource obdHourSource = ObdHourSource.builder()
                .carId("60b5e2ecef4248fe9b3075e4cf79f3de")
                .time(1531396745000L)
                .totalDistance(25242.22F)
                .build();
        ObdHourLatestData obdHourLatestData = ObdHourLatestData.builder()
                .carId("60b5e2ecef4248fe9b3075e4cf79f3de")
                .time(1531396732000L)
                .totalDistance(25241.283F)
                .build();
        Map map = new HashMap();
        map.put("travel_speed", "250");
        ExceptionCodeStatus codeStatus=obdHourCalcBiz.commpareExceptionWithEachData(obdHourSource, obdHourLatestData, map);
        System.out.println(codeStatus);
    }
}

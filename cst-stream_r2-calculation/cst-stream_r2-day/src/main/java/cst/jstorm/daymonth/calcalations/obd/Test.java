package cst.jstorm.daymonth.calcalations.obd;

import com.cst.jstorm.commons.stream.constants.ExceptionCodeStatus;
import com.cst.stream.stathour.obd.ObdDayLatestData;
import com.cst.stream.stathour.obd.ObdHourSource;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Johnney.Chiu
 * create on 2018/8/9 19:24
 * @Description
 * @title
 */
public class Test {

    public static void main(String ... args){
        Test test = new Test();
        ObdDayLatestData obdDayLatestData = ObdDayLatestData.builder()
                .sameEngineSpeedCount(2)
                .engineSpeed(100)
                .speed(100)
                .build();
        ObdHourSource obdHourSource = ObdHourSource.builder()
                .speed(100)
                .engineSpeed(100)
                .build();
        System.out.println(test.calcLatestData(obdDayLatestData, obdHourSource, new HashMap<String,String>(),ExceptionCodeStatus.CALC_DATA)
        .getSameEngineSpeedCount());
    }

    public ObdDayLatestData calcLatestData(ObdDayLatestData latestData, ObdHourSource obdHourSource, Map map, ExceptionCodeStatus status) {

        int sameEngineSpeedCount = latestData.getSameEngineSpeedCount();
        if(obdHourSource.getEngineSpeed()<=0)
            sameEngineSpeedCount = 0;
        else {
            if (obdHourSource.getSpeed().intValue() == latestData.getSpeed().intValue()
                    && obdHourSource.getEngineSpeed().intValue() == latestData.getEngineSpeed().intValue()) {
                sameEngineSpeedCount += 1;
            } else {
                sameEngineSpeedCount = 0;
            }
        }
        latestData.setSameEngineSpeedCount(sameEngineSpeedCount);

        return latestData;
    }
}

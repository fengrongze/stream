package cst.jstorm.daymonth.calcalations.obd;

import com.cst.stream.common.JsonHelper;
import com.cst.stream.stathour.obd.ObdDayTransfor;
import com.fasterxml.jackson.core.type.TypeReference;
import com.cst.jstorm.commons.stream.operations.IntegrityDealInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * obd完整性计算
 */
public class ObdDayIntegrityBiz implements IntegrityDealInterface<ObdDayTransfor> {
    private final static Logger logger = LoggerFactory.getLogger(ObdDayIntegrityBiz.class);
    @Override
    public Map<String, String> convertData2Map(ObdDayTransfor obdDayTransfor) {
        Map<String,String> map = null;
           /* if(null!=obdDayTransfor){
                map = new HashMap<>();
                map.put("carId",obdDayTransfor.getCarId());
                map.put("din",obdDayTransfor.getDin());
                map.put("duration",String.valueOf(obdDayTransfor.getDuration()));
                map.put("fee",String.valueOf(obdDayTransfor.getFee()));
                map.put("fuel",String.valueOf(obdDayTransfor.getFuel()));
                map.put("fuelPerHundred",String.valueOf(obdDayTransfor.getFuelPerHundred()));
                map.put("isDrive",String.valueOf(obdDayTransfor.getIsDrive()));
                map.put("isHighSpeed",String.valueOf(obdDayTransfor.getIsHighSpeed()));
                map.put("isNightDrive",String.valueOf(obdDayTransfor.getIsNightDrive()));
                map.put("maxSpeed",String.valueOf(obdDayTransfor.getMaxSpeed()));
                map.put("mileage",String.valueOf(obdDayTransfor.getMileage()));
                map.put("motormeterDistance",String.valueOf(obdDayTransfor.getMotormeterDistance()));
                map.put("totalFuel",String.valueOf(obdDayTransfor.getTotalFuel()));
                map.put("runTotalTime",String.valueOf(obdDayTransfor.getRunTotalTime()));
            }*/

        return map;
    }

    @Override
    public ObdDayTransfor initMsg2Data(String msg) {
            ObdDayTransfor obdDayTransfor = null;
            try {
                obdDayTransfor = JsonHelper.toBeanWithoutException(msg, new TypeReference<ObdDayTransfor>() {
                });
            } catch (Exception e) {
                logger.warn("msg convert map error",e);
            }
            return obdDayTransfor;
    }
}

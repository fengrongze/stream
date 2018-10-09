package cst.jstorm.hour.calcalations.obd;

import com.cst.stream.common.JsonHelper;
import com.cst.stream.stathour.obd.ObdHourTransfor;
import com.fasterxml.jackson.core.type.TypeReference;
import com.cst.jstorm.commons.stream.operations.IntegrityDealInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * obd小时完整性计算
 */
public class ObdHourIntegrityBiz implements IntegrityDealInterface<ObdHourTransfor> {
    private final static Logger logger = LoggerFactory.getLogger(ObdHourIntegrityBiz.class);
    @Override
    public Map<String, String> convertData2Map(ObdHourTransfor obdHourTransfor) {
        Map<String,String> map = null;
           /* if(null!=obdHourTransfor){
                map = new HashMap<>();
                map.put("carId",obdHourTransfor.getCarId());
                map.put("din",obdHourTransfor.getDin());
                map.put("duration",String.valueOf(obdHourTransfor.getDuration()));
                map.put("fee",String.valueOf(obdHourTransfor.getFee()));
                map.put("fuel",String.valueOf(obdHourTransfor.getFuel()));
                map.put("fuelPerHundred",String.valueOf(obdHourTransfor.getFuelPerHundred()));
                map.put("isDrive",String.valueOf(obdHourTransfor.getIsDrive()));
                map.put("isHighSpeed",String.valueOf(obdHourTransfor.getIsHighSpeed()));
                map.put("maxSpeed",String.valueOf(obdHourTransfor.getMaxSpeed()));
                map.put("mileage",String.valueOf(obdHourTransfor.getMileage()));
                map.put("motormeterDistance",String.valueOf(obdHourTransfor.getMotormeterDistance()));
                map.put("totalFuel",String.valueOf(obdHourTransfor.getTotalFuel()));
                map.put("runTotalTime",String.valueOf(obdHourTransfor.getRunTotalTime()));
            }*/

        return map;
    }

    @Override
    public ObdHourTransfor initMsg2Data(String msg) {
            ObdHourTransfor obdHourTransfor = null;
            try {
                obdHourTransfor = JsonHelper.toBeanWithoutException(msg, new TypeReference<ObdHourTransfor>() {
                });
            } catch (Exception e) {
                logger.warn("msg convert map error",e);
            }
            return obdHourTransfor;
    }


}

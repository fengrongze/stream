package cst.jstorm.hour.calcalations.integrated;

import com.cst.jstorm.commons.stream.operations.IntegratedDealTransforInterface;
import com.cst.stream.stathour.integrated.HourIntegratedTransfor;

import java.util.HashMap;
import java.util.Map;

public class HourIntegratedCalcBiz  implements IntegratedDealTransforInterface<HourIntegratedTransfor> {
    @Override
    public Map<String, String> convertData2Map(HourIntegratedTransfor transfor) {
        Map<String,String> transforMap = null;
        if(null!=transfor) {
            transforMap = new HashMap<>();
            //obd
            transforMap.put("time",null!=transfor.getTime()?String.valueOf(transfor.getTime()):null);
            transforMap.put("carId",transfor.getCarId());
            transforMap.put("din",transfor.getDin());
            transforMap.put("duration",String.valueOf(transfor.getDuration()));
            transforMap.put("fee",String.valueOf(transfor.getFee()));
            transforMap.put("fuel",String.valueOf(transfor.getFuel()));
            transforMap.put("fuelPerHundred",String.valueOf(transfor.getFuelPerHundred()));
            transforMap.put("isDrive",String.valueOf(transfor.getIsDrive()));
            transforMap.put("isHighSpeed",String.valueOf(transfor.getIsHighSpeed()));
            transforMap.put("maxSpeed",String.valueOf(transfor.getMaxSpeed()));
            transforMap.put("mileage",String.valueOf(transfor.getMileage()));
            transforMap.put("motormeterDistance",String.valueOf(transfor.getMotormeterDistance()));
            transforMap.put("totalFuel",String.valueOf(transfor.getTotalFuel()));
            transforMap.put("runTotalTime",String.valueOf(transfor.getRunTotalTime()));
            transforMap.put("toolingProbability",String.valueOf(transfor.getToolingProbability()));
            transforMap.put("averageSpeed",String.valueOf(transfor.getAverageSpeed()));

            //de
            transforMap.put("rapidAccelerationCount",String.valueOf(transfor.getRapidAccelerationCount()));
            transforMap.put("rapidDecelerationCount",String.valueOf(transfor.getRapidDecelerationCount()));
            transforMap.put("sharpTurnCount",String.valueOf(transfor.getSharpTurnCount()));

        }
        return transforMap;
    }
}
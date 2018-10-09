package cst.jstorm.daymonth.calcalations.integrated;

import com.cst.jstorm.commons.stream.operations.IntegratedDealTransforInterface;
import com.cst.stream.stathour.integrated.DayIntegratedTransfor;

import java.util.HashMap;
import java.util.Map;

public class DayIntegratedCalcBiz implements IntegratedDealTransforInterface<DayIntegratedTransfor> {
    @Override
    public Map<String, String> convertData2Map(DayIntegratedTransfor transfor) {
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
            transforMap.put("isNightDrive",String.valueOf(transfor.getIsNightDrive()));
            transforMap.put("maxSpeed",String.valueOf(transfor.getMaxSpeed()));
            transforMap.put("mileage",String.valueOf(transfor.getMileage()));
            transforMap.put("motormeterDistance",String.valueOf(transfor.getMotormeterDistance()));
            transforMap.put("totalFuel",String.valueOf(transfor.getTotalFuel()));
            transforMap.put("runTotalTime",String.valueOf(transfor.getRunTotalTime()));
            transforMap.put("toolingProbability",String.valueOf(transfor.getToolingProbability()));
            transforMap.put("averageSpeed",String.valueOf(transfor.getAverageSpeed()));

            //am
            transforMap.put("ignition",String.valueOf(transfor.getIgnition()));
            transforMap.put("flameOut",String.valueOf(transfor.getFlameOut()));
            transforMap.put("insertNum",String.valueOf(transfor.getInsertNum()));
            transforMap.put("collision",String.valueOf(transfor.getCollision()));
            transforMap.put("overSpeed",String.valueOf(transfor.getOverSpeed()));
            transforMap.put("isMissing",String.valueOf(transfor.getIsMissing()));
            transforMap.put("pulloutTimes",String.valueOf(transfor.getPulloutTimes()));
            transforMap.put("isFatigue",String.valueOf(transfor.getIsFatigue()));

            //de
            transforMap.put("rapidAccelerationCount",String.valueOf(transfor.getRapidAccelerationCount()));
            transforMap.put("rapidDecelerationCount",String.valueOf(transfor.getRapidDecelerationCount()));
            transforMap.put("sharpTurnCount",String.valueOf(transfor.getSharpTurnCount()));

        }
        return transforMap;
    }
}

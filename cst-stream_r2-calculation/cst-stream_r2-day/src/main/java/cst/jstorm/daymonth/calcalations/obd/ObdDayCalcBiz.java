package cst.jstorm.daymonth.calcalations.obd;

import com.cst.stream.common.DateTimeUtil;
import com.cst.stream.common.MathUtils;
import com.cst.stream.stathour.CSTData;
import com.cst.stream.stathour.dormancy.DormancySource;
import com.cst.stream.stathour.obd.ObdDayLatestData;
import com.cst.stream.stathour.obd.ObdDayTransfor;
import com.cst.stream.stathour.obd.ObdHourSource;
import com.cst.stream.stathour.obd.ObdHourTransfor;
import com.cst.jstorm.commons.stream.constants.ExceptionCodeStatus;
import com.cst.jstorm.commons.stream.operations.DataDealTransforInterface;
import com.cst.jstorm.commons.stream.operations.DataTransforInterface;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;

import static com.cst.jstorm.commons.stream.operations.jump.JumpCharge.jumpNomalChargeWithMoreData;
import static com.cst.stream.common.BusinessMathUtil.calcAvarageSpeed;
import static com.cst.stream.common.BusinessMathUtil.calcFuelPerHundred;
import static com.cst.stream.common.MathUtils.round;

/**
 * @author Johnney.chiu
 * create on 2017/12/20 16:24
 * @Description 天计算
 */
@NoArgsConstructor
@Slf4j
public class ObdDayCalcBiz implements
        DataDealTransforInterface<ObdDayTransfor,ObdHourSource,ObdDayLatestData> {


    public ObdDayCalcBiz addMileage(ObdHourTransfor obdHourTransfor, Float totalDistance){
        if (totalDistance < 0) return this;

        BigDecimal total =BigDecimal.valueOf(totalDistance==null?0f:totalDistance);
        BigDecimal oldTotal =BigDecimal.valueOf(obdHourTransfor.getTotalDistance()==null
                ?0f:obdHourTransfor.getTotalDistance());
        float temp = total.subtract(oldTotal).floatValue();
       /* if(temp<0)
            return this;*/
        obdHourTransfor.setMileage(BigDecimal.valueOf(obdHourTransfor.getMileage()).add(total.subtract(oldTotal)).floatValue());
        return this;
    }
    public ObdDayCalcBiz addFuel(ObdHourTransfor obdHourTransfor, Float totalFuel){
        if (totalFuel < 0) return this;
        BigDecimal total =BigDecimal.valueOf(totalFuel==null?0f:totalFuel);
        BigDecimal oldTotal =BigDecimal.valueOf(obdHourTransfor.getTotalFuel()==null?0f:obdHourTransfor.getTotalFuel());
        float temp = total.subtract(oldTotal).floatValue();
        /*if(temp<0)
            return this;*/
        obdHourTransfor.setFuel(BigDecimal.valueOf(obdHourTransfor.getFuel()).add(total.subtract(oldTotal)).floatValue());
        return this;
    }
    public ObdDayCalcBiz addDuration(ObdHourTransfor obdHourTransfor, Integer runTotalTime){
        if(runTotalTime<0) return this;
        obdHourTransfor.setDuration(obdHourTransfor.getDuration()+(runTotalTime - obdHourTransfor.getRunTotalTime()));
        return this;
    }
    public ObdDayCalcBiz chargeMaxSpeed(ObdHourTransfor obdHourTransfor, Integer speed){
        if(speed>obdHourTransfor.getMaxSpeed())
           obdHourTransfor.setMaxSpeed(Float.valueOf(speed));
        return this;
    }

    public ObdDayCalcBiz chargeIsHighSpeed(ObdHourTransfor obdHourTransfor, Integer speed, Map map){
        //System.out.println(map.get("highSpeedStandard"));
        if(speed>Integer.valueOf((String)map.get("highSpeedStandard")))
            obdHourTransfor.setIsHighSpeed(1);
        return this;
    }
    public boolean chargeIsHighSpeed(Integer sourceSpeed,Map map){
        //System.out.println(map.get("highSpeedStandard"));
        if(sourceSpeed>Integer.valueOf((String)map.get("highSpeedStandard")))
            return true;
        return false;
    }

    public ObdDayCalcBiz chargeIsNightDrive(ObdHourTransfor obdHourTransfor, long time, Integer runTotalTime, Map map){
        if(runTotalTime<0)
            return this;

        return this;
    }

    public int chargeIsNightDrive(long time,Integer runTotalTime,Map map){
        if(runTotalTime<0)
            return 0;
        String str = DateTimeUtil.toLongTimeString(time, DateTimeUtil.DEFAULT_DATE_DEFULT);
        int hour=Integer.parseInt(str.substring(11,13));
        if(hour>=Integer.valueOf((String)map.get("night_high"))||hour<Integer.valueOf((String)map.get("night_low"))){
            return 1;
        }
        return 0;
    }


    public ObdDayCalcBiz chargeIsDrive(ObdHourTransfor obdHourTransfor, ObdHourSource obdHourSource){
        if(obdHourTransfor.getIsDrive()==1)
            return this;
        if(obdHourSource.getSpeed()>0||obdHourTransfor.getRunTotalTime()-obdHourSource.getRunTotalTime()>0)
            obdHourTransfor.setIsDrive(1);
        return this;
    }

    public ObdDayCalcBiz clacFee(ObdHourTransfor obdHourTransfor, Float totalDistance, Map<String,Object> map){
        try {
            BigDecimal total = BigDecimal.valueOf(totalDistance);
            BigDecimal oldTotal = BigDecimal.valueOf(obdHourTransfor.getTotalDistance());
            obdHourTransfor.setFee(BigDecimal.valueOf(obdHourTransfor.getFee()).add(total.subtract(oldTotal).
                    multiply(BigDecimal.valueOf(NumberUtils.toFloat((String) map.get("fuelPrice"),7.8F)))).floatValue());
        }catch (Exception e){
            log.error("fuelPrice is {}",map.get("fuelPrice"));
        }
        return this;
    }
    public float clacFee(Float oldFuel,Float newFuel,Map<String,Object> map){
        if(newFuel<oldFuel)
            return 0f;
        try {
            BigDecimal total = BigDecimal.valueOf(newFuel);
            BigDecimal oldTotal = BigDecimal.valueOf(oldFuel);
            if(null!=map.get("fuelPrice")){
                if(NumberUtils.toFloat(map.get("fuelPrice").toString())<=0){
                    map.put("fuelPrice","7.8");
                }
            }else{
                map.put("fuelPrice","7.8");
            }
            return (float) round((total.subtract(oldTotal)).multiply(BigDecimal.valueOf(NumberUtils.toFloat((String) map.get("fuelPrice"),7.8F))), 3);
        }catch (Exception e){
            log.error("fuelPrice is {}",map.get("fuelPrice"));
        }
        return 0f;
    }


    public ObdDayCalcBiz initMyTransfor(ObdHourTransfor obdHourTransfor, ObdHourSource obdHourSource){
        obdHourTransfor.setCarId(obdHourSource.getCarId());
        obdHourTransfor.setTime(obdHourSource.getTime());
        obdHourTransfor.setDin(obdHourSource.getDin());
        obdHourTransfor.setSpeed(obdHourSource.getSpeed());
        if(obdHourTransfor.getMaxSpeed()<=0)
            obdHourTransfor.setMaxSpeed((float)obdHourSource.getSpeed());
        obdHourTransfor.setTotalDistance(obdHourSource.getTotalDistance());
        obdHourTransfor.setTotalFuel(obdHourSource.getTotalFuel());
        obdHourTransfor.setRunTotalTime(obdHourSource.getRunTotalTime());
        obdHourTransfor.setMotormeterDistance(obdHourSource.getMotormeterDistance());

        return this;
    }

    public ObdDayCalcBiz dataJumped(ObdHourTransfor obdHourTransfor, ObdHourSource obdHourSource, Map map){
        return this;
    }
    public ObdDayCalcBiz initTime(ObdHourTransfor obdHourTransfor, long uploadTime) {
        obdHourTransfor.setTime(uploadTime);
        return this;
    }


    public ObdDayCalcBiz calcData(ObdHourTransfor obdHourTransfor, ObdHourSource obdHourSource, Map<String, Object> other) {
        if(StringUtils.isEmpty(obdHourTransfor.getDin())||!obdHourTransfor.getDin().equals(obdHourSource.getDin()))
            this.initMyTransfor(obdHourTransfor,obdHourSource);

        this.addDuration(obdHourTransfor,obdHourSource.getRunTotalTime())
                .addFuel(obdHourTransfor,obdHourSource.getTotalFuel())
                .addMileage(obdHourTransfor,obdHourSource.getTotalDistance())
                .chargeIsDrive(obdHourTransfor,obdHourSource)
                .chargeIsHighSpeed(obdHourTransfor,obdHourSource.getSpeed(), other)
                .chargeIsNightDrive(obdHourTransfor,obdHourSource.getTime(), obdHourSource.getRunTotalTime(), other)
                .chargeMaxSpeed(obdHourTransfor,obdHourSource.getSpeed())
                .clacFee(obdHourTransfor,obdHourSource.getTotalDistance(),other)
                //最后在初始化一遍数据
                .initMyTransfor(obdHourTransfor,obdHourSource);
        return this;
    }


    @Override
    public ExceptionCodeStatus commpareExceptionWithEachData(ObdHourSource obdHourSource, ObdDayLatestData latestData, Map map) {
        return jumpNomalChargeWithMoreData(obdHourSource, map, obdHourSource.getSpeed(), obdHourSource.getTotalDistance(),
                latestData.getTime(), latestData.getTotalDistance());
    }

    @Override
    public ObdDayLatestData calcLatestData(ObdDayLatestData latestData, ObdHourSource obdHourSource,Map map,ExceptionCodeStatus status) {
        latestData.setCarId(obdHourSource.getCarId());
        if(status!=ExceptionCodeStatus.CALC_NO_TIME)
            latestData.setTime(obdHourSource.getTime());

        latestData.setTotalFuel(obdHourSource.getTotalFuel());
        latestData.setTotalDistance(obdHourSource.getTotalDistance());
        latestData.setRunTotalTime(obdHourSource.getRunTotalTime());

        latestData.setMotormeterDistance(obdHourSource.getMotormeterDistance());
        if(latestData.getIsHighSpeed()!=1&&chargeIsHighSpeed(obdHourSource.getSpeed(),map))
            latestData.setIsHighSpeed(1);
        if(latestData.getIsNightDrive()!=1)
            latestData.setIsNightDrive(chargeIsNightDrive(obdHourSource.getTime(), obdHourSource.getRunTotalTime(), map));
        latestData.setMaxSpeed(Float.valueOf(Math.max(latestData.getMaxSpeed(),obdHourSource.getSpeed())));

        if(latestData.getIsDrive()!=1&&obdHourSource.getSpeed()>0)
            latestData.setIsDrive(1);

        if (!StringUtils.isBlank(obdHourSource.getDin()) && !obdHourSource.getDin().equals(latestData.getDin())) {
            latestData.setDinChange(1);
        }
        latestData.setDin(obdHourSource.getDin());
        int sameEngineSpeedCount = latestData.getSameEngineSpeedCount() == null ? 0 : latestData.getSameEngineSpeedCount();
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
        if(sameEngineSpeedCount>=NumberUtils.toInt((String)map.get("tooling_probability_count"),40)){
            latestData.setToolingProbability(1f);
        }
        latestData.setEngineSpeed(obdHourSource.getEngineSpeed());
        latestData.setSpeed(obdHourSource.getSpeed());
        return latestData;
    }

    @Override
    public ObdDayLatestData initLatestData(ObdHourSource obdHourSource,Map map,ObdDayLatestData latestData) {
        ObdDayLatestData obdDayLatestData = ObdDayLatestData.builder()
                .maxSpeed(Float.valueOf(obdHourSource.getSpeed()))
                .carId(obdHourSource.getCarId())
                .isHighSpeed(chargeIsHighSpeed(obdHourSource.getSpeed(), map) ? 1 : 0)
                .isNightDrive(chargeIsNightDrive(obdHourSource.getTime(), obdHourSource.getRunTotalTime(), map))
                .motormeterDistance(obdHourSource.getMotormeterDistance())
                .runTotalTime(obdHourSource.getRunTotalTime())
                .time(obdHourSource.getTime())
                .totalDistance(obdHourSource.getTotalDistance())
                .totalFuel(obdHourSource.getTotalFuel())
                .isDrive(obdHourSource.getSpeed() > 0 ? 1 : 0)
                .dinChange(0)
                .toolingProbability(0f)
                .sameEngineSpeedCount(0)
                .build();
        if (latestData != null) {
            if (!StringUtils.isBlank(obdHourSource.getDin()) && !obdHourSource.getDin().equals(latestData.getDin())) {
                obdDayLatestData.setDinChange(1);
            }
            int sameEngineSpeedCount = latestData.getSameEngineSpeedCount()==null?0:latestData.getSameEngineSpeedCount();
            if(obdHourSource.getEngineSpeed()<=0)
                sameEngineSpeedCount = 0;
            else {
                if (obdHourSource.getSpeed().intValue() == latestData.getSpeed().intValue()
                        && obdHourSource.getEngineSpeed().intValue() == latestData.getEngineSpeed().intValue()) {
                    sameEngineSpeedCount += 1;
                }else{
                    sameEngineSpeedCount = 0;
                }
            }
            obdDayLatestData.setSameEngineSpeedCount(sameEngineSpeedCount);

            if(sameEngineSpeedCount>=NumberUtils.toInt((String)map.get("tooling_probability_count"),40)){
                obdDayLatestData.setToolingProbability(1f);
            }
        }
        obdDayLatestData.setSpeed(obdHourSource.getSpeed());
        obdDayLatestData.setEngineSpeed(obdHourSource.getEngineSpeed());
        obdDayLatestData.setDin(obdHourSource.getDin());
        return obdDayLatestData;
    }

    @Override
    public ObdDayTransfor calcTransforData(ObdHourSource latestFirstData, ObdDayLatestData latestData, ObdHourSource obdHourSource, Map map) {
        float mil = (float) round(BigDecimal.valueOf(obdHourSource.getTotalDistance()).subtract(BigDecimal.valueOf(latestFirstData.getTotalDistance())), 3);
        float fuel = (float) round(BigDecimal.valueOf(obdHourSource.getTotalFuel()).subtract(BigDecimal.valueOf(latestFirstData.getTotalFuel())), 3);
        float fee = clacFee( latestFirstData.getTotalFuel(),obdHourSource.getTotalFuel(), map);
        return ObdDayTransfor.builder()
                .totalFuel(obdHourSource.getTotalFuel())
                .totalDistance(obdHourSource.getTotalDistance())
                .speed(obdHourSource.getSpeed())
                .runTotalTime(obdHourSource.getRunTotalTime())
                .motormeterDistance(obdHourSource.getMotormeterDistance())
                .mileage(mil<0?0:mil)
                .maxSpeed(latestData.getMaxSpeed())
                .isNightDrive(latestData.getIsNightDrive())
                .isHighSpeed(latestData.getIsHighSpeed())
                .isDrive(latestData.getIsDrive())
                .fuel(fuel<0?0:fuel)
                .time(latestFirstData.getTime())
                .din(latestFirstData.getDin())
                .carId(latestFirstData.getCarId())
                .duration(obdHourSource.getRunTotalTime()-latestFirstData.getRunTotalTime())
                .fee(fee<0?0:fee)
                .fuelPerHundred(calcFuelPerHundred(fuel,mil))
                .toolingProbability(latestData.getToolingProbability())
                .dinChange(latestData.getDinChange())
                .averageSpeed(calcAvarageSpeed(mil,obdHourSource.getRunTotalTime()-latestFirstData.getRunTotalTime()))
                .powerConsumption(-1F)
                .build();
    }

    @Override
    public ObdDayTransfor initFromTempTransfer(ObdDayLatestData latestData,ObdHourSource obdHourSource,Map map,long supplyTime) {
        return ObdDayTransfor.builder()
                .carId(latestData.getCarId())
                .din(latestData.getDin())
                .time(supplyTime)
                .duration(0)
                .fee(0f)
                .fuel(0f)
                .isDrive(0)
                .isHighSpeed(0)
                .isNightDrive(0)
                .maxSpeed(0f)
                .mileage(0f)
                .motormeterDistance(obdHourSource.getMotormeterDistance())
                .runTotalTime(obdHourSource.getRunTotalTime())
                .speed(0)
                .totalDistance(obdHourSource.getTotalDistance())
                .totalFuel(obdHourSource.getTotalFuel())
                .fuelPerHundred(0f)
                .averageSpeed(0f)
                .dinChange(0)
                .toolingProbability(0f)
                .powerConsumption(-1F)
                .build();
    }

    @Override
    public Map<String, String> convertData2Map(ObdDayTransfor transfor,ObdDayLatestData latestData) {
        Map<String,String> map = null;
        if(null==transfor)
            transfor = initTransfor(latestData);

            map = new HashMap<>();
            map.put("time",null!=transfor.getTime()?String.valueOf(transfor.getTime()):null);
            map.put("carId",transfor.getCarId());
            map.put("din",transfor.getDin());
            map.put("duration",String.valueOf(transfor.getDuration()));
            map.put("fee",String.valueOf(transfor.getFee()));
            map.put("fuel",String.valueOf(transfor.getFuel()));
            map.put("fuelPerHundred",String.valueOf(transfor.getFuelPerHundred()));
            map.put("isDrive",String.valueOf(transfor.getIsDrive()));
            map.put("isHighSpeed",String.valueOf(transfor.getIsHighSpeed()));
            map.put("isNightDrive",String.valueOf(transfor.getIsNightDrive()));
            map.put("maxSpeed",String.valueOf(transfor.getMaxSpeed()));
            map.put("mileage",String.valueOf(transfor.getMileage()));
            map.put("motormeterDistance",String.valueOf(transfor.getMotormeterDistance()));
            map.put("totalFuel",String.valueOf(transfor.getTotalFuel()));
            map.put("runTotalTime",String.valueOf(transfor.getRunTotalTime()));
            map.put("toolingProbability",String.valueOf(transfor.getToolingProbability()));
            map.put("averageSpeed",String.valueOf(transfor.getAverageSpeed()));
        return map;
    }
    private ObdDayTransfor initTransfor(ObdDayLatestData latestData){

        Float motormeterDistance =latestData!=null?latestData.getMotormeterDistance():0f;
        Integer  RunTotalTime =  latestData!=null?latestData.getRunTotalTime():0;
        Float  totalDistance = latestData!=null? latestData.getTotalDistance():0f;
        Float  totalFuel = latestData!=null?latestData.getTotalFuel():0f;

       return ObdDayTransfor.builder()
                .carId(null)
                .din(null)
                .time(null)
                .duration(0)
                .fee(0f)
                .fuel(0f)
                .isDrive(0)
                .isHighSpeed(0)
                .isNightDrive(0)
                .maxSpeed(0f)
                .mileage(0f)
                .motormeterDistance(motormeterDistance)
                .runTotalTime(RunTotalTime)
                .speed(0)
                .totalDistance(totalDistance)
                .totalFuel(totalFuel)
                .fuelPerHundred(0f)
                .averageSpeed(0f)
                .dinChange(0)
                .toolingProbability(0f)
                .powerConsumption(-1F)
                .build();
    }
    @Override
    public ObdDayTransfor calcIntegrityData(ObdHourSource latestFirstData, ObdDayLatestData latestData, ObdDayTransfor obdDayTransfor,Map map) {
        if(null!=obdDayTransfor){
            return obdDayTransfor;
        }else {
            float mil = (float) round(BigDecimal.valueOf(latestData.getTotalDistance()).subtract(BigDecimal.valueOf(latestFirstData.getTotalDistance())), 3);
            float fuel = (float) round(BigDecimal.valueOf(latestData.getTotalFuel()).subtract(BigDecimal.valueOf(latestFirstData.getTotalFuel())), 3);
            float fee = clacFee(latestFirstData.getTotalFuel(), latestData.getTotalFuel(), map);
            return ObdDayTransfor.builder()
                    .totalFuel(latestData.getTotalFuel())
                    .totalDistance(latestData.getTotalDistance())
                    .speed(latestData.getSpeed())
                    .runTotalTime(latestData.getRunTotalTime())
                    .motormeterDistance(latestData.getMotormeterDistance())
                    .mileage(mil < 0 ? 0 : mil)
                    .maxSpeed(latestData.getMaxSpeed())
                    .isNightDrive(latestData.getIsNightDrive())
                    .isHighSpeed(latestData.getIsHighSpeed())
                    .isDrive(latestData.getIsDrive())
                    .fuel(fuel < 0 ? 0 : fuel)
                    .time(latestFirstData.getTime())
                    .din(latestFirstData.getDin())
                    .carId(latestFirstData.getCarId())
                    .duration(latestData.getRunTotalTime() - latestFirstData.getRunTotalTime())
                    .fee(fee < 0 ? 0 : fee)
                    .fuelPerHundred(calcFuelPerHundred(fuel, mil))
                    .toolingProbability(latestData.getToolingProbability())
                    .dinChange(latestData.getDinChange())
                    .averageSpeed(calcAvarageSpeed(mil, latestData.getRunTotalTime() - latestFirstData.getRunTotalTime()))
                    .powerConsumption(-1F)
                    .build();
        }
    }

    @Override
    public ObdDayTransfor initFromDormancy(ObdDayLatestData latestData,long supplyTime) {
        ObdHourSource obdHourSource = ObdHourSource.builder()
                .totalDistance(latestData.getTotalDistance())
                .totalFuel(latestData.getTotalFuel())
                .runTotalTime(latestData.getRunTotalTime())
                .motormeterDistance(latestData.getMotormeterDistance())
                .build();
        return initFromTempTransfer(latestData,obdHourSource,null,supplyTime);
    }
}

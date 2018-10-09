package cst.jstorm.daymonth.calcalations.obd;

import com.cst.stream.common.MathUtils;
import com.cst.stream.stathour.obd.ObdDayTransfor;
import com.cst.stream.stathour.obd.ObdYearTransfor;
import com.cst.jstorm.commons.stream.operations.DataAccumulationTransforInterface;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;

import static com.cst.stream.common.BusinessMathUtil.calcAvarageSpeed;
import static com.cst.stream.common.BusinessMathUtil.calcFuelPerHundred;
import static com.cst.stream.common.MathUtils.round;

/**
 * @author Johnney.chiu
 * create on 2017/12/20 16:34
 * @Description
 */

@NoArgsConstructor
@Slf4j
public class ObdYearCalcBiz implements DataAccumulationTransforInterface<ObdYearTransfor,ObdDayTransfor> {


    @Override
    public ObdYearTransfor initTransforDataBySource(ObdDayTransfor obdDayTransfor, Map map) {
        ObdYearTransfor obdYearTransfor = ObdYearTransfor.builder()
                .carId(obdDayTransfor.getCarId())
                .din(obdDayTransfor.getDin())
                .duration(obdDayTransfor.getDuration())
                .fee(obdDayTransfor.getFee())
                .fuel(obdDayTransfor.getFuel())
                .driveNum(obdDayTransfor.getIsDrive()==null?0:obdDayTransfor.getIsDrive())
                .highSpeedNum(obdDayTransfor.getIsHighSpeed()==null?0:obdDayTransfor.getIsHighSpeed())
                .maxSpeed(obdDayTransfor.getMaxSpeed())
                .speed(obdDayTransfor.getSpeed())
                .mileage(obdDayTransfor.getMileage())
                .time(obdDayTransfor.getTime())
                .totalDistance(obdDayTransfor.getTotalDistance())
                .totalFuel(obdDayTransfor.getTotalFuel())
                .nightDriveNum(obdDayTransfor.getIsNightDrive()==null?0:obdDayTransfor.getIsHighSpeed())
                .runTotalTime(obdDayTransfor.getRunTotalTime())
                .motormeterDistance(obdDayTransfor.getMotormeterDistance())
                .fuelPerHundred(calcFuelPerHundred(obdDayTransfor.getFuel(),obdDayTransfor.getMileage()))
                .dinChangeNum(obdDayTransfor.getDinChange()==null?0:obdDayTransfor.getDinChange())
                .toolingProbability(obdDayTransfor.getToolingProbability())
                .averageSpeed(calcAvarageSpeed(obdDayTransfor.getMileage(),obdDayTransfor.getDuration()))
                .powerConsumption(-1F)
                .build();
        return obdYearTransfor;
    }

    @Override
    public ObdYearTransfor initTransforDataByLatest(ObdYearTransfor latestTransforData, Map map,Long time){
        ObdYearTransfor obdYearTransfor = ObdYearTransfor.builder()
                .carId(latestTransforData.getCarId())
                .din(latestTransforData.getDin())
                .duration(0)
                .fee(0f)
                .fuel(0f)
                .driveNum(0)
                .highSpeedNum(0)
                .nightDriveNum(0)
                .maxSpeed(0f)
                .speed(0)
                .mileage(0f)
                .time(time)
                .totalDistance(latestTransforData.getTotalDistance())
                .totalFuel(latestTransforData.getTotalFuel())
                .motormeterDistance(latestTransforData.getMotormeterDistance())
                .fuelPerHundred(0f)
                .dinChangeNum(0)
                .toolingProbability(0f)
                .averageSpeed(0f)
                .powerConsumption(-1F)
                .build();
        return obdYearTransfor;
    }

    @Override
    public ObdYearTransfor calcTransforData(ObdYearTransfor latestTransforData, ObdDayTransfor source, Map map) {
        ObdYearTransfor obdYearTransfor = ObdYearTransfor.builder()
                .carId(source.getCarId())
                .din(source.getDin())
                .time(source.getTime())
                .totalDistance(source.getTotalDistance())
                .totalFuel(source.getTotalFuel())
                .speed(source.getSpeed())
                .motormeterDistance(source.getMotormeterDistance())
                .runTotalTime(source.getRunTotalTime())
                .powerConsumption(-1F)
                .build();
        if(latestTransforData.getHighSpeedNum()!=null)
            if(1==source.getIsHighSpeed())
                obdYearTransfor.setDriveNum(latestTransforData.getDriveNum() + 1);
            else{
                obdYearTransfor.setDriveNum(latestTransforData.getDriveNum());
            }
        else{
            if (1 == source.getIsHighSpeed()) {
                obdYearTransfor.setHighSpeedNum(1);
            } else {
                obdYearTransfor.setHighSpeedNum(0);
            }
        }
        if(latestTransforData.getDriveNum()!=null)
            if(1==source.getIsDrive())
                obdYearTransfor.setDriveNum(latestTransforData.getDriveNum() + 1);
            else{
                obdYearTransfor.setDriveNum(latestTransforData.getDriveNum());
            }
        else{
            if (1 == source.getIsDrive()) {
                obdYearTransfor.setDriveNum(1);
            } else {
                obdYearTransfor.setDriveNum(0);
            }
        }
        if(latestTransforData.getNightDriveNum()!=null)
            if(1==source.getIsNightDrive())
                obdYearTransfor.setNightDriveNum(latestTransforData.getNightDriveNum() + 1);
            else
                obdYearTransfor.setNightDriveNum(latestTransforData.getNightDriveNum());
        else{
            if (1 == source.getIsNightDrive()) {
                obdYearTransfor.setNightDriveNum(1);
            } else {
                obdYearTransfor.setNightDriveNum(0);
            }
        }
        if(latestTransforData.getMaxSpeed()==null){
            obdYearTransfor.setMaxSpeed(source.getMaxSpeed());
        }else{
            obdYearTransfor.setMaxSpeed(Math.max(latestTransforData.getMaxSpeed(), source.getMaxSpeed()));
        }

        int duration = latestTransforData.getDuration() + source.getDuration();
        obdYearTransfor.setDuration(duration);
        obdYearTransfor.setFee((float) round(BigDecimal.valueOf(latestTransforData.getFee()).add(BigDecimal.valueOf(source.getFee())), 3));
        float fuel = (float) round(BigDecimal.valueOf(latestTransforData.getFuel()).add(BigDecimal.valueOf(source.getFuel())), 3);
        float mileage = (float) round(BigDecimal.valueOf(latestTransforData.getMileage()).add(BigDecimal.valueOf(source.getMileage())), 3);
        obdYearTransfor.setFuel(fuel);
        obdYearTransfor.setMileage(mileage);
        obdYearTransfor.setFuelPerHundred(calcFuelPerHundred(fuel,mileage));
        if(latestTransforData.getDinChangeNum()!=null)
            if(1==source.getDinChange())
                obdYearTransfor.setDinChangeNum(latestTransforData.getDinChangeNum() + 1);
            else
                obdYearTransfor.setDinChangeNum(latestTransforData.getDinChangeNum() );
        else {
            if (1 == source.getDinChange()) {
                obdYearTransfor.setDinChangeNum(1);
            } else {
                obdYearTransfor.setDinChangeNum(0);
            }
        }
        obdYearTransfor.setAverageSpeed(calcAvarageSpeed(mileage,duration));
        obdYearTransfor.setToolingProbability(Math.max(source.getToolingProbability(),latestTransforData.getToolingProbability()));

        return obdYearTransfor;
    }
}

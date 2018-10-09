package cst.jstorm.daymonth.calcalations.obd;

import com.cst.jstorm.commons.stream.operations.DataAccumulationTransforInterface;
import com.cst.stream.stathour.obd.ObdDayTransfor;
import com.cst.stream.stathour.obd.ObdMonthTransfor;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
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
public class ObdMonthCalcBiz implements DataAccumulationTransforInterface<ObdMonthTransfor,ObdDayTransfor> {


    @Override
    public ObdMonthTransfor initTransforDataBySource(ObdDayTransfor obdDayTransfor, Map map) {
        ObdMonthTransfor obdMonthTransfor = ObdMonthTransfor.builder()
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
        return obdMonthTransfor;
    }

    @Override
    public ObdMonthTransfor initTransforDataByLatest(ObdMonthTransfor latestTransforData, Map map,Long time){
        ObdMonthTransfor obdMonthTransfor = ObdMonthTransfor.builder()
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
                .fuelPerHundred(0f)
                .dinChangeNum(0)
                .toolingProbability(0f)
                .averageSpeed(0f)
                .powerConsumption(-1F)
                .build();
        return obdMonthTransfor;
    }

    @Override
    public ObdMonthTransfor calcTransforData(ObdMonthTransfor latestTransforData, ObdDayTransfor source, Map map) {
        ObdMonthTransfor obdMonthTransfor = ObdMonthTransfor.builder()
                .carId(source.getCarId())
                .din(source.getDin())
                .time(source.getTime())
                .speed(source.getSpeed())
                .totalDistance(source.getTotalDistance())
                .runTotalTime(source.getRunTotalTime())
                .motormeterDistance(source.getMotormeterDistance())
                .totalFuel(source.getTotalFuel())
                .powerConsumption(-1F)
                .build();

        if(latestTransforData.getHighSpeedNum()!=null) {
            if(1==source.getIsHighSpeed()){
                obdMonthTransfor.setHighSpeedNum(latestTransforData.getHighSpeedNum() + 1);
            }else{
                obdMonthTransfor.setHighSpeedNum(latestTransforData.getHighSpeedNum());
            }
        }
        else {
            if (1 == source.getIsHighSpeed()) {
                obdMonthTransfor.setHighSpeedNum(1);
            } else {
                obdMonthTransfor.setHighSpeedNum(0);
            }
        }

        if(latestTransforData.getDriveNum()!=null){
            if(1==source.getIsDrive())
                obdMonthTransfor.setDriveNum(latestTransforData.getDriveNum() + 1);
            else{
                obdMonthTransfor.setDriveNum(latestTransforData.getDriveNum());
            }
        }
        else{
            if (1 == source.getIsDrive()) {
                obdMonthTransfor.setDriveNum(1);
            } else {
                obdMonthTransfor.setDriveNum(0);
            }
        }

        if(latestTransforData.getNightDriveNum()!=null) {
            if(1==source.getIsNightDrive())
                obdMonthTransfor.setNightDriveNum(latestTransforData.getNightDriveNum() + 1);
            else
                obdMonthTransfor.setNightDriveNum(latestTransforData.getNightDriveNum());
        }
        else{
            if (1 == source.getIsNightDrive()) {
                obdMonthTransfor.setNightDriveNum(1);
            } else {
                obdMonthTransfor.setNightDriveNum(0);
            }
        }

        if(latestTransforData.getMaxSpeed()==null)
            obdMonthTransfor.setMaxSpeed(source.getMaxSpeed());
        else
            obdMonthTransfor.setMaxSpeed(Math.max(latestTransforData.getMaxSpeed(), source.getMaxSpeed()));
        int duration = latestTransforData.getDuration() + source.getDuration();
        obdMonthTransfor.setDuration(duration);
        obdMonthTransfor.setFee((float) round(BigDecimal.valueOf(latestTransforData.getFee()).add(BigDecimal.valueOf(source.getFee())), 3));
        float fuel = (float) round(BigDecimal.valueOf(latestTransforData.getFuel()).add(BigDecimal.valueOf(source.getFuel())), 3);
        float mileage = (float) round(BigDecimal.valueOf(latestTransforData.getMileage()).add(BigDecimal.valueOf(source.getMileage())), 3);
        obdMonthTransfor.setFuel(fuel);
        obdMonthTransfor.setMileage(mileage);
        obdMonthTransfor.setFuelPerHundred(calcFuelPerHundred(fuel,mileage));
        if(latestTransforData.getDinChangeNum()!=null) {
            if(1==source.getDinChange())
                obdMonthTransfor.setDinChangeNum(latestTransforData.getDinChangeNum() + 1);
            else
                obdMonthTransfor.setDinChangeNum(latestTransforData.getDinChangeNum() );
        }
        else{
            if (1 == source.getDinChange()) {
                obdMonthTransfor.setDinChangeNum(1);
            } else {
                obdMonthTransfor.setDinChangeNum(0);
            }
        }

        obdMonthTransfor.setAverageSpeed(calcAvarageSpeed(mileage,duration));
        obdMonthTransfor.setToolingProbability(Math.max(source.getToolingProbability(),latestTransforData.getToolingProbability()));


        return obdMonthTransfor;
    }
}

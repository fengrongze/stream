package cst.jstorm.daymonth.calcalations.mileage;

import com.cst.jstorm.commons.stream.operations.DataAccumulationTransforInterface;
import com.cst.stream.stathour.mileage.MileageDayTransfor;
import com.cst.stream.stathour.mileage.MileageMonthTransfor;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.Map;

import static com.cst.stream.common.MathUtils.round;

/**
 * @author Johnney.chiu
 * create on 2017/12/20 16:34
 * @Description
 */

@NoArgsConstructor
@Slf4j
public class MileageMonthCalcBiz implements
        DataAccumulationTransforInterface<MileageMonthTransfor,MileageDayTransfor> {

    @Override
    public MileageMonthTransfor initTransforDataBySource(MileageDayTransfor mileageDayTransfor, Map map) {
        return MileageMonthTransfor.builder()
                .carId(mileageDayTransfor.getCarId())
                .gpsSpeed(mileageDayTransfor.getGpsSpeed())
                .milDuration(mileageDayTransfor.getMilDuration())
                .milFee(mileageDayTransfor.getMilFee())
                .milFuel(mileageDayTransfor.getMilFuel())
                .milGpsMaxSpeed(mileageDayTransfor.getMilGpsMaxSpeed())
                .milGpsMileage(mileageDayTransfor.getMilGpsMileage())
                .milGpsTotalDistance(mileageDayTransfor.getMilGpsTotalDistance())
                .milObdMaxSpeed(mileageDayTransfor.getMilObdMaxSpeed())
                .milObdMileage(mileageDayTransfor.getMilObdMileage())
                .milObdTotalDistance(mileageDayTransfor.getMilObdTotalDistance())
                .milPanelMileage(mileageDayTransfor.getMilPanelMileage())
                .milRunTotalTime(mileageDayTransfor.getMilRunTotalTime())
                .milTotalFuel(mileageDayTransfor.getMilTotalFuel())
                .obdSpeed(mileageDayTransfor.getObdSpeed())
                .panelDistance(mileageDayTransfor.getPanelDistance())
                .time(mileageDayTransfor.getTime())
                .build();
    }

    @Override
    public MileageMonthTransfor initTransforDataByLatest(MileageMonthTransfor mileageMonthTransfor, Map map, Long time) {
        return MileageMonthTransfor.builder()
                .carId(mileageMonthTransfor.getCarId())
                .time(time)
                .milDuration(0)
                .milFee(0D)
                .milFuel(0D)
                .milGpsMaxSpeed(0f)
                .milGpsMileage(0D)
                .milObdMaxSpeed(0f)
                .milObdMileage(0D)
                .milPanelMileage(0D)
                .milGpsTotalDistance(0D)
                .milObdTotalDistance(0D)
                .milRunTotalTime(0L)
                .milTotalFuel(0D)
                .gpsSpeed(0)
                .obdSpeed(0)
                .panelDistance(0D)
                .build();
    }

    @Override
    public MileageMonthTransfor calcTransforData(MileageMonthTransfor mileageMonthTransfor, MileageDayTransfor mileageDayTransfor, Map map) {
        return MileageMonthTransfor.builder()
                .milPanelMileage(round(BigDecimal.valueOf(mileageDayTransfor.getMilPanelMileage()).add(BigDecimal.valueOf(mileageMonthTransfor.getMilPanelMileage())), 3))
                .milObdTotalDistance(mileageDayTransfor.getMilObdTotalDistance())
                .milObdMileage(round(BigDecimal.valueOf(mileageDayTransfor.getMilObdMileage()).add(BigDecimal.valueOf(mileageMonthTransfor.getMilObdMileage())), 3))
                .milObdMaxSpeed(Math.max(mileageDayTransfor.getMilObdMaxSpeed(),mileageMonthTransfor.getMilObdMaxSpeed()))
                .milGpsTotalDistance(mileageDayTransfor.getMilGpsTotalDistance())
                .milGpsMileage( round(BigDecimal.valueOf(mileageDayTransfor.getMilGpsMileage()).add(BigDecimal.valueOf(mileageMonthTransfor.getMilGpsMileage())), 3))
                .milGpsMaxSpeed(Math.max(mileageDayTransfor.getMilGpsMaxSpeed(),mileageMonthTransfor.getMilGpsMaxSpeed()))
                .milFuel( round(BigDecimal.valueOf(mileageMonthTransfor.getMilFuel()).add(BigDecimal.valueOf(mileageDayTransfor.getMilFuel())), 3))
                .milFee( round(BigDecimal.valueOf(mileageDayTransfor.getMilFee()).add(BigDecimal.valueOf(mileageMonthTransfor.getMilFee())), 3))
                .milDuration(mileageDayTransfor.getMilDuration()+mileageMonthTransfor.getMilDuration())
                .milRunTotalTime(mileageDayTransfor.getMilRunTotalTime())
                .milTotalFuel(mileageDayTransfor.getMilTotalFuel())
                .time(mileageDayTransfor.getTime())
                .gpsSpeed(mileageDayTransfor.getGpsSpeed())
                .carId(mileageDayTransfor.getCarId())
                .obdSpeed(mileageDayTransfor.getObdSpeed())
                .panelDistance(mileageDayTransfor.getPanelDistance())
                .build();
    }
}

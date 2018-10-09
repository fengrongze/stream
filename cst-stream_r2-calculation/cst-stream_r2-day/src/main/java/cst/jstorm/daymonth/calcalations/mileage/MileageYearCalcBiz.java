package cst.jstorm.daymonth.calcalations.mileage;

import com.cst.jstorm.commons.stream.operations.DataAccumulationTransforInterface;
import com.cst.stream.stathour.mileage.MileageDayTransfor;
import com.cst.stream.stathour.mileage.MileageYearTransfor;
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
public class MileageYearCalcBiz implements
        DataAccumulationTransforInterface<MileageYearTransfor,MileageDayTransfor> {

    @Override
    public MileageYearTransfor initTransforDataBySource(MileageDayTransfor mileageDayTransfor, Map map) {
        return MileageYearTransfor.builder()
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
    public MileageYearTransfor initTransforDataByLatest(MileageYearTransfor mileageYearTransfor, Map map, Long time) {
        return MileageYearTransfor.builder()
                .carId(mileageYearTransfor.getCarId())
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
    public MileageYearTransfor calcTransforData(MileageYearTransfor mileageYearTransfor, MileageDayTransfor mileageDayTransfor, Map map) {
        return MileageYearTransfor.builder()
                .milPanelMileage(round(BigDecimal.valueOf(mileageDayTransfor.getMilPanelMileage()).add(BigDecimal.valueOf(mileageYearTransfor.getMilPanelMileage())), 3))
                .milObdTotalDistance(mileageDayTransfor.getMilObdTotalDistance())
                .milObdMileage(round(BigDecimal.valueOf(mileageDayTransfor.getMilObdMileage()).add(BigDecimal.valueOf(mileageYearTransfor.getMilObdMileage())), 3))
                .milObdMaxSpeed(Math.max(mileageDayTransfor.getMilObdMaxSpeed(),mileageYearTransfor.getMilObdMaxSpeed()))
                .milGpsTotalDistance(mileageDayTransfor.getMilGpsTotalDistance())
                .milGpsMileage(round(BigDecimal.valueOf(mileageDayTransfor.getMilGpsMileage()).add(BigDecimal.valueOf(mileageYearTransfor.getMilGpsMileage())), 3))
                .milGpsMaxSpeed(Math.max(mileageDayTransfor.getMilGpsMaxSpeed(),mileageYearTransfor.getMilGpsMaxSpeed()))
                .milFuel( round(BigDecimal.valueOf(mileageYearTransfor.getMilFuel()).add(BigDecimal.valueOf(mileageDayTransfor.getMilFuel())), 3))
                .milFee(round(BigDecimal.valueOf(mileageDayTransfor.getMilFee()).add(BigDecimal.valueOf(mileageYearTransfor.getMilFee())), 3))
                .milTotalFuel(mileageDayTransfor.getMilTotalFuel())
                .milDuration(mileageDayTransfor.getMilDuration()+mileageYearTransfor.getMilDuration())
                .milRunTotalTime(mileageDayTransfor.getMilRunTotalTime())
                .time(mileageDayTransfor.getTime())
                .carId(mileageDayTransfor.getCarId())
                .gpsSpeed(mileageDayTransfor.getGpsSpeed())
                .obdSpeed(mileageDayTransfor.getObdSpeed())
                .panelDistance(mileageDayTransfor.getPanelDistance())
                .build();
    }
}

package cst.jstorm.daymonth.calcalations.mileage;

import com.cst.jstorm.commons.stream.constants.ExceptionCodeStatus;
import com.cst.jstorm.commons.stream.operations.DataDealTransforInterface;
import com.cst.stream.stathour.mileage.*;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.Map;

import static com.cst.jstorm.commons.stream.constants.ExceptionCodeStatus.CALC_DATA;
import static com.cst.jstorm.commons.stream.operations.jump.JumpCharge.jumpNomalChargeWithMoreData;
import static com.cst.stream.common.BusinessMathUtil.clacFee;
import static com.cst.stream.common.MathUtils.round;

/**
 * @author Johnney.chiu
 * create on 2017/12/20 16:24
 * @Description 天计算
 */
@NoArgsConstructor
@Slf4j
public class MileageDayCalcBiz implements
        DataDealTransforInterface<MileageDayTransfor,MileageHourSource,MileageDayLatestData> {

    @Override
    public ExceptionCodeStatus commpareExceptionWithEachData(MileageHourSource mileageHourSource, MileageDayLatestData mileageDayLatestData, Map map) {
        ExceptionCodeStatus status = jumpNomalChargeWithMoreData(mileageHourSource, map
                , mileageHourSource.getGpsSpeed(), mileageHourSource.getMilGpsTotalDistance(), mileageDayLatestData.getTime()
                , mileageDayLatestData.getMilGpsTotalDistance());

        if(status!=CALC_DATA)
            return status;
        status = jumpNomalChargeWithMoreData(mileageHourSource, map, mileageHourSource.getObdSpeed(), mileageHourSource.getMilObdTotalDistance(),
                mileageDayLatestData.getObdSpeed(), mileageDayLatestData.getMilObdTotalDistance());
        return status;

    }

    @Override
    public MileageDayLatestData calcLatestData(MileageDayLatestData mileageDayLatestData, MileageHourSource mileageHourSource, Map map, ExceptionCodeStatus exceptionCodeStatus) {
        mileageDayLatestData.setCarId(mileageHourSource.getCarId());
        if(exceptionCodeStatus!=ExceptionCodeStatus.CALC_NO_TIME)
            mileageDayLatestData.setTime(mileageHourSource.getTime());
        mileageDayLatestData.setGpsSpeed(mileageHourSource.getGpsSpeed());
        mileageDayLatestData.setMilGpsTotalDistance(mileageHourSource.getMilGpsTotalDistance());
        mileageDayLatestData.setMilObdTotalDistance(mileageHourSource.getMilObdTotalDistance());
        mileageDayLatestData.setMilRunTotalTime(mileageHourSource.getMilRunTotalTime());
        mileageDayLatestData.setMilTotalFuel(mileageHourSource.getMilTotalFuel());
        mileageDayLatestData.setObdSpeed(mileageHourSource.getObdSpeed());
        mileageDayLatestData.setPanelDistance(mileageHourSource.getPanelDistance());
        mileageDayLatestData.setMilGpsMaxSpeed(Float.valueOf(Math.max(mileageDayLatestData.getGpsSpeed(),mileageHourSource.getGpsSpeed())));
        mileageDayLatestData.setMilObdMaxSpeed(Float.valueOf(Math.max(mileageDayLatestData.getObdSpeed(),mileageHourSource.getObdSpeed())));
        if(mileageDayLatestData.getPanelFlag()==null||mileageDayLatestData.getPanelFlag()==0)
            mileageDayLatestData.setPanelFlag(mileageHourSource.getPanelDistance()==null||mileageHourSource.getPanelDistance()==-1?1:0);
        return mileageDayLatestData;
    }

    @Override
    public MileageDayLatestData initLatestData(MileageHourSource mileageHourSource, Map map, MileageDayLatestData mileageDayLatestData) {
        MileageDayLatestData latestData = MileageDayLatestData.builder()
                .carId(mileageHourSource.getCarId())
                .gpsSpeed(mileageHourSource.getGpsSpeed())
                .milGpsMaxSpeed((float)mileageHourSource.getGpsSpeed())
                .milGpsTotalDistance(mileageHourSource.getMilGpsTotalDistance())
                .milObdMaxSpeed((float)mileageHourSource.getObdSpeed())
                .milObdTotalDistance(mileageHourSource.getMilObdTotalDistance())
                .milRunTotalTime(mileageHourSource.getMilRunTotalTime())
                .milTotalFuel(mileageHourSource.getMilTotalFuel())
                .obdSpeed(mileageHourSource.getObdSpeed())
                .panelDistance(mileageHourSource.getPanelDistance())
                .panelFlag(mileageHourSource.getPanelDistance()==-1?1:0)
                .time(mileageHourSource.getTime())
                .build();
        return latestData;
    }

    @Override
    public MileageDayTransfor calcTransforData(MileageHourSource latestFirstData, MileageDayLatestData latestData, MileageHourSource mileageHourSource, Map map) {
        double obdMil =  round(BigDecimal.valueOf(mileageHourSource.getMilObdTotalDistance())
                .subtract(BigDecimal.valueOf(latestFirstData.getMilObdTotalDistance())), 3);
        double gpsMil =  round(BigDecimal.valueOf(mileageHourSource.getMilGpsTotalDistance())
                .subtract(BigDecimal.valueOf(latestFirstData.getMilGpsTotalDistance())), 3);
        double panelMil = -1D;
        if (latestData.getPanelFlag() == null||latestData.getPanelFlag()==0) {
            panelMil = round(BigDecimal.valueOf(mileageHourSource.getPanelDistance())
                    .subtract(BigDecimal.valueOf(latestFirstData.getPanelDistance())), 3);
        }
        double fuel =   round(BigDecimal.valueOf(mileageHourSource.getMilTotalFuel()).subtract(BigDecimal.valueOf(latestFirstData.getMilTotalFuel())), 3);
        double fee = clacFee( latestFirstData.getMilTotalFuel(),mileageHourSource.getMilTotalFuel(), map);
        MileageDayTransfor mileageDayTransfor = MileageDayTransfor.builder()
                .carId(latestFirstData.getCarId())
                .time(latestFirstData.getTime())
                .milDuration((int)(mileageHourSource.getMilRunTotalTime()-latestFirstData.getMilRunTotalTime()))
                .milFee(fee)
                .milFuel(fuel)
                .milGpsTotalDistance(mileageHourSource.getMilGpsTotalDistance())
                .milObdTotalDistance(mileageHourSource.getMilObdTotalDistance())
                .milRunTotalTime(mileageHourSource.getMilRunTotalTime())
                .milTotalFuel(mileageHourSource.getMilTotalFuel())
                .milGpsMaxSpeed(latestData.getMilGpsMaxSpeed()==null?0:latestData.getMilGpsMaxSpeed())
                .milGpsMileage(gpsMil)
                .milObdMaxSpeed(latestData.getMilObdMaxSpeed()==null?0:latestData.getMilObdMaxSpeed())
                .milObdMileage(obdMil)
                .milPanelMileage(panelMil)
                .gpsSpeed(mileageHourSource.getGpsSpeed()==null?0:mileageHourSource.getGpsSpeed())
                .obdSpeed(mileageHourSource.getObdSpeed()==null?0:mileageHourSource.getObdSpeed())
                .panelDistance(mileageHourSource.getPanelDistance())
                .build();
        return mileageDayTransfor;
    }

    @Override
    public MileageDayTransfor initFromTempTransfer(MileageDayLatestData mileageDayLatestData, MileageHourSource mileageHourSource, Map map, long supplyTime) {
        return MileageDayTransfor.builder()
                .milObdMileage(0D)
                .milObdMaxSpeed(0f)
                .milGpsMileage(0D)
                .milGpsMaxSpeed(0f)
                .milFuel(0D)
                .milFee(0D)
                .milDuration(0)
                .milPanelMileage(0D)
                .milGpsTotalDistance(mileageHourSource.getMilGpsTotalDistance())
                .milObdTotalDistance(mileageHourSource.getMilObdTotalDistance())
                .milRunTotalTime(mileageHourSource.getMilRunTotalTime())
                .milTotalFuel(mileageHourSource.getMilTotalFuel())
                .carId(mileageHourSource.getCarId())
                .time(supplyTime)
                .gpsSpeed(0)
                .obdSpeed(0)
                .panelDistance(mileageHourSource.getPanelDistance())
                .build();
    }

    @Override
    public Map<String, String> convertData2Map(MileageDayTransfor mileageDayTransfor, MileageDayLatestData mileageDayLatestData) {
        return null;
    }

    @Override
    public MileageDayTransfor calcIntegrityData(MileageHourSource mileageHourSource, MileageDayLatestData latestData,
                                                MileageDayTransfor mileageDayTransfor, Map map) {
        if(null!=mileageDayTransfor)
            return mileageDayTransfor;
        double obdMil =   round(BigDecimal.valueOf(mileageHourSource.getMilObdTotalDistance())
                .subtract(BigDecimal.valueOf(latestData.getMilObdTotalDistance())), 3);
        double gpsMil =   round(BigDecimal.valueOf(mileageHourSource.getMilGpsTotalDistance())
                .subtract(BigDecimal.valueOf(latestData.getMilGpsTotalDistance())), 3);
        double panelMil = -1D;
        if (latestData.getPanelFlag() == null||latestData.getPanelFlag()==0) {
            panelMil =   round(BigDecimal.valueOf(mileageHourSource.getPanelDistance())
                    .subtract(BigDecimal.valueOf(latestData.getPanelDistance())), 3);
        }

        double fuel =   round(BigDecimal.valueOf(mileageHourSource.getMilTotalFuel()).subtract(BigDecimal.valueOf(latestData.getMilTotalFuel())), 3);
        double fee = clacFee( latestData.getMilTotalFuel(),mileageHourSource.getMilTotalFuel(), map);
        return MileageDayTransfor.builder()
                .carId(latestData.getCarId())
                .time(latestData.getTime())
                .milDuration((int)(mileageHourSource.getMilRunTotalTime()-latestData.getMilRunTotalTime()))
                .milFee(fee)
                .milFuel(fuel)
                .milGpsMaxSpeed(latestData.getMilGpsMaxSpeed()==null?0:latestData.getMilGpsMaxSpeed())
                .milGpsMileage(gpsMil)
                .milObdMaxSpeed(latestData.getMilObdMaxSpeed()==null?0:latestData.getMilObdMaxSpeed())
                .milGpsTotalDistance(mileageHourSource.getMilGpsTotalDistance())
                .milObdTotalDistance(mileageHourSource.getMilObdTotalDistance())
                .milRunTotalTime(mileageHourSource.getMilRunTotalTime())
                .milTotalFuel(mileageHourSource.getMilTotalFuel())
                .milObdMileage(obdMil)
                .milPanelMileage(panelMil)
                .gpsSpeed(mileageHourSource.getGpsSpeed()==null?0:mileageHourSource.getGpsSpeed())
                .obdSpeed(mileageHourSource.getObdSpeed()==null?0:mileageHourSource.getObdSpeed())
                .panelDistance(mileageHourSource.getPanelDistance())
                .build();
    }

    @Override
    public MileageDayTransfor initFromDormancy(MileageDayLatestData latestData, long supplyTime) {
        return MileageDayTransfor.builder()
                .carId(latestData.getCarId())
                .time(supplyTime)
                .milDuration(0)
                .milFee(0D)
                .milFuel(0D)
                .milGpsTotalDistance(0D)
                .milObdTotalDistance(0D)
                .milRunTotalTime(0L)
                .milTotalFuel(0D)
                .milGpsMaxSpeed(0f)
                .milGpsMileage(0D)
                .milObdMaxSpeed(0f)
                .milObdMileage(0D)
                .milPanelMileage(0D)
                .gpsSpeed(0)
                .obdSpeed(0)
                .panelDistance(0D)
                .build();
    }
}

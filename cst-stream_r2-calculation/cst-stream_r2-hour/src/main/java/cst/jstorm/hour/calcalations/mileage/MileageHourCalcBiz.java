package cst.jstorm.hour.calcalations.mileage;

import com.cst.jstorm.commons.stream.constants.ExceptionCodeStatus;
import com.cst.jstorm.commons.stream.operations.DataDealTransforInterface;
import com.cst.jstorm.commons.stream.operations.DataTransforInterface;
import com.cst.stream.stathour.mileage.MileageHourLatestData;
import com.cst.stream.stathour.mileage.MileageHourSource;
import com.cst.stream.stathour.mileage.MileageHourTransfor;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.math.NumberUtils;

import java.math.BigDecimal;
import java.util.Map;

import static com.cst.jstorm.commons.stream.constants.ExceptionCodeStatus.CALC_DATA;
import static com.cst.jstorm.commons.stream.operations.jump.JumpCharge.jumpNomalChargeWithMoreData;
import static com.cst.stream.common.BusinessMathUtil.clacFee;
import static com.cst.stream.common.MathUtils.round;

/**
 * @author Johnney.chiu
 * create on 2017/12/20 16:24
 * @Description
 */
@NoArgsConstructor
@Slf4j
public class MileageHourCalcBiz implements DataTransforInterface<MileageHourTransfor,MileageHourSource>,
        DataDealTransforInterface<MileageHourTransfor,MileageHourSource,MileageHourLatestData> {

    @Override
    public ExceptionCodeStatus commpareExceptionWithEachData(MileageHourSource mileageHourSource, MileageHourLatestData mileageHourLatestData, Map map) {
        ExceptionCodeStatus status = jumpNomalChargeWithMoreData(mileageHourSource, map
                , mileageHourSource.getGpsSpeed(), mileageHourSource.getMilGpsTotalDistance(), mileageHourLatestData.getTime()
                , mileageHourLatestData.getMilGpsTotalDistance());

        if(status!=CALC_DATA)
            return status;
        status = jumpNomalChargeWithMoreData(mileageHourSource, map, mileageHourSource.getObdSpeed(), mileageHourSource.getMilObdTotalDistance(),
                mileageHourLatestData.getObdSpeed(), mileageHourLatestData.getMilObdTotalDistance());
        return status;

    }

    @Override
    public MileageHourLatestData calcLatestData(MileageHourLatestData mileageHourLatestData, MileageHourSource mileageHourSource, Map map, ExceptionCodeStatus exceptionCodeStatus) {
        mileageHourLatestData.setCarId(mileageHourSource.getCarId());
        if(exceptionCodeStatus!=ExceptionCodeStatus.CALC_NO_TIME)
            mileageHourLatestData.setTime(mileageHourSource.getTime());
        mileageHourLatestData.setGpsSpeed(mileageHourSource.getGpsSpeed());
        mileageHourLatestData.setMilGpsTotalDistance(mileageHourSource.getMilGpsTotalDistance());
        mileageHourLatestData.setMilObdTotalDistance(mileageHourSource.getMilObdTotalDistance());
        mileageHourLatestData.setMilRunTotalTime(mileageHourSource.getMilRunTotalTime());
        mileageHourLatestData.setMilTotalFuel(mileageHourSource.getMilTotalFuel());
        mileageHourLatestData.setObdSpeed(mileageHourSource.getObdSpeed());
        mileageHourLatestData.setPanelDistance(mileageHourSource.getPanelDistance());
        mileageHourLatestData.setMilGpsMaxSpeed(Float.valueOf(Math.max(mileageHourLatestData.getGpsSpeed(),mileageHourSource.getGpsSpeed())));
        mileageHourLatestData.setMilObdMaxSpeed(Float.valueOf(Math.max(mileageHourLatestData.getObdSpeed(),mileageHourSource.getObdSpeed())));
        if(mileageHourLatestData.getPanelFlag()==null||mileageHourLatestData.getPanelFlag()==0)
            mileageHourLatestData.setPanelFlag(mileageHourSource.getPanelDistance()==null||mileageHourSource.getPanelDistance()==-1?1:0);
        return mileageHourLatestData;

    }

    @Override
    public MileageHourLatestData initLatestData(MileageHourSource mileageHourSource, Map map, MileageHourLatestData mileageHourLatestData) {
        MileageHourLatestData latestData = MileageHourLatestData.builder()
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
    public MileageHourTransfor calcTransforData(MileageHourSource latestFirstData, MileageHourLatestData latestData,
                                                MileageHourSource mileageHourSource, Map map) {
        double obdMil =  round(BigDecimal.valueOf(mileageHourSource.getMilObdTotalDistance())
                .subtract(BigDecimal.valueOf(latestFirstData.getMilObdTotalDistance())), 3);
        double gpsMil =  round(BigDecimal.valueOf(mileageHourSource.getMilGpsTotalDistance())
                .subtract(BigDecimal.valueOf(latestFirstData.getMilGpsTotalDistance())), 3);
        double panelMil = -1D;
        if (latestData.getPanelFlag() == null||latestData.getPanelFlag()==0) {
            panelMil = round(BigDecimal.valueOf(mileageHourSource.getPanelDistance())
                    .subtract(BigDecimal.valueOf(latestFirstData.getPanelDistance())), 3);
        }

        double fuel =  round(BigDecimal.valueOf(mileageHourSource.getMilTotalFuel()).subtract(BigDecimal.valueOf(latestFirstData.getMilTotalFuel())), 3);
        double fee = clacFee( latestFirstData.getMilTotalFuel(),mileageHourSource.getMilTotalFuel(), map);

        MileageHourTransfor mileageHourTransfor = MileageHourTransfor.builder()
                .carId(latestFirstData.getCarId())
                .time(latestFirstData.getTime())
                .milDuration((int)(mileageHourSource.getMilRunTotalTime()-latestFirstData.getMilRunTotalTime()))
                .milFee(fee)
                .milFuel(fuel)
                .milGpsMaxSpeed(latestData.getMilGpsMaxSpeed())
                .milGpsMileage(gpsMil)
                .milObdMaxSpeed(latestData.getMilObdMaxSpeed())
                .milObdMileage(obdMil)
                .milPanelMileage(panelMil)
                .gpsSpeed(mileageHourSource.getGpsSpeed())
                .milGpsTotalDistance(mileageHourSource.getMilGpsTotalDistance())
                .milObdTotalDistance(mileageHourSource.getMilObdTotalDistance())
                .milRunTotalTime(mileageHourSource.getMilRunTotalTime())
                .milTotalFuel(mileageHourSource.getMilTotalFuel())
                .obdSpeed(mileageHourSource.getObdSpeed())
                .panelDistance(mileageHourSource.getPanelDistance())
                .build();
        return mileageHourTransfor;
    }


    @Override
    public MileageHourTransfor initFromTempTransfer(MileageHourLatestData mileageHourLatestData, MileageHourSource mileageHourSource, Map map, long supplyTime) {
        return MileageHourTransfor.builder()
                .milObdMileage(0D)
                .milObdMaxSpeed(0f)
                .milGpsMileage(0D)
                .milGpsMaxSpeed(0f)
                .milFuel(0D)
                .milFee(0D)
                .milDuration(0)
                .milPanelMileage(0D)
                .carId(mileageHourSource.getCarId())
                .time(supplyTime)
                .gpsSpeed(0)
                .milGpsTotalDistance(mileageHourSource.getMilGpsTotalDistance())
                .milObdTotalDistance(mileageHourSource.getMilObdTotalDistance())
                .milRunTotalTime(mileageHourSource.getMilRunTotalTime())
                .milTotalFuel(mileageHourSource.getMilTotalFuel())
                .obdSpeed(0)
                .panelDistance(mileageHourSource.getPanelDistance())
                .build();
    }

    @Override
    public Map<String, String> convertData2Map(MileageHourTransfor mileageHourTransfor, MileageHourLatestData mileageHourLatestData) {
        return null;
    }

    @Override
    public MileageHourTransfor calcIntegrityData(MileageHourSource mileageHourSource, MileageHourLatestData latestData, MileageHourTransfor mileageHourTransfor, Map map) {
        if(null!=mileageHourTransfor)
            return mileageHourTransfor;
        double obdMil =   round(BigDecimal.valueOf(mileageHourSource.getMilObdTotalDistance())
                .subtract(BigDecimal.valueOf(latestData.getMilObdTotalDistance())), 3);
        double gpsMil =   round(BigDecimal.valueOf(mileageHourSource.getMilGpsTotalDistance())
                .subtract(BigDecimal.valueOf(latestData.getMilGpsTotalDistance())), 3);
        double panelMil = -1D;
        if (latestData.getPanelFlag() == null||latestData.getPanelFlag()==0) {
            panelMil =   round(BigDecimal.valueOf(mileageHourSource.getPanelDistance())
                    .subtract(BigDecimal.valueOf(latestData.getPanelDistance())), 3);
        }
        double fuel =  round(BigDecimal.valueOf(mileageHourSource.getMilTotalFuel()).subtract(BigDecimal.valueOf(latestData.getMilTotalFuel())), 3);
        double fee = clacFee( latestData.getMilTotalFuel(),mileageHourSource.getMilTotalFuel(), map);
        return MileageHourTransfor.builder()
                .carId(latestData.getCarId())
                .time(latestData.getTime())
                .milDuration((int)(mileageHourSource.getMilRunTotalTime()-latestData.getMilRunTotalTime()))
                .milFee(fee)
                .milFuel(fuel)
                .milGpsMaxSpeed(latestData.getMilGpsMaxSpeed())
                .milGpsMileage(gpsMil)
                .milObdMaxSpeed(latestData.getMilObdMaxSpeed())
                .milObdMileage(obdMil)
                .milPanelMileage(panelMil)
                .gpsSpeed(mileageHourSource.getGpsSpeed())
                .milGpsTotalDistance(mileageHourSource.getMilGpsTotalDistance())
                .milObdTotalDistance(mileageHourSource.getMilObdTotalDistance())
                .milRunTotalTime(mileageHourSource.getMilRunTotalTime())
                .milTotalFuel(mileageHourSource.getMilTotalFuel())
                .obdSpeed(mileageHourSource.getObdSpeed())
                .panelDistance(mileageHourSource.getPanelDistance())
                .build();

    }

    @Override
    public MileageHourTransfor initFromDormancy(MileageHourLatestData mileageHourLatestData, long l1) {
        return null;
    }





    @Override
    public void execute(MileageHourTransfor mileageHourTransfor, MileageHourSource mileageHourSource, Map<String, Object> map) {

    }

    @Override
    public MileageHourTransfor init(MileageHourSource mileageHourSource, Map<String, Object> map) {
        return null;
    }

    @Override
    public MileageHourTransfor initOffet(MileageHourTransfor mileageHourTransfor, MileageHourSource mileageHourSource, Map<String, Object> map) {
        return null;
    }

    @Override
    public MileageHourTransfor initFromTransfer(MileageHourTransfor mileageHourTransfor, Map<String, Object> map) {
        return null;
    }
}

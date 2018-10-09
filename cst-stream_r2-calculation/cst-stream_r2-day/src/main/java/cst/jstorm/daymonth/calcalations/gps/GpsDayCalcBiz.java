package cst.jstorm.daymonth.calcalations.gps;

import com.cst.stream.stathour.CSTData;
import com.cst.stream.stathour.dormancy.DormancySource;
import com.cst.stream.stathour.gps.GpsDayLatestData;
import com.cst.stream.stathour.gps.GpsDayTransfor;
import com.cst.stream.stathour.gps.GpsHourSource;
import com.cst.stream.stathour.gps.GpsHourTransfor;
import com.cst.jstorm.commons.stream.constants.ExceptionCodeStatus;
import com.cst.jstorm.commons.stream.operations.DataDealTransforInterface;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.cst.jstorm.commons.stream.operations.InvalidDataCharge.invalidTooLessThanTime;

/**
 * @author Johnney.chiu
 * create on 2017/12/20 16:18
 * @Description gps天计算
 */
@NoArgsConstructor
@Slf4j
public class GpsDayCalcBiz implements
        DataDealTransforInterface<GpsDayTransfor,GpsHourSource,GpsDayLatestData> {

    private GpsDayCalcBiz addGpsCount(GpsHourTransfor gpsHourTransfor) {
        gpsHourTransfor.setGpsCount(gpsHourTransfor.getGpsCount()+1);
        return this;
    }


    private GpsDayCalcBiz maxSatellitesCalc(GpsHourTransfor gpsHourTransfor, Integer satellites) {
        if(satellites>gpsHourTransfor.getMaxSatelliteNum()) {
            gpsHourTransfor.setMaxSatelliteNum(satellites);
        }
        return this;
    }

    private GpsDayCalcBiz timeSetting(GpsHourTransfor gpsHourTransfor, Long time) {
        gpsHourTransfor.setTime(time);
        return this;
    }

    @Override
    public ExceptionCodeStatus commpareExceptionWithEachData(GpsHourSource gpsHourSource, GpsDayLatestData latestData, Map map) {
        if(gpsHourSource.getTime()<latestData.getTime()) {
            log.warn("upload gps time exception,original data {},latest data :{}",gpsHourSource,latestData);
            return ExceptionCodeStatus.CALC_NO_TIME;
        }

        return ExceptionCodeStatus.CALC_DATA;
    }

    @Override
    public GpsDayLatestData calcLatestData(GpsDayLatestData latestData, GpsHourSource gpsHourSource, Map map,ExceptionCodeStatus status) {
        latestData.setCarId(gpsHourSource.getCarId());
        if(status!=ExceptionCodeStatus.CALC_NO_TIME)
            latestData.setTime(gpsHourSource.getTime());
        latestData.setGpsCount(latestData.getGpsCount()+1);
        latestData.setIsNonLocal(0);
        latestData.setMaxSatelliteNum(Math.max(latestData.getMaxSatelliteNum(),gpsHourSource.getSatellites()));
        return latestData;
    }

    @Override
    public GpsDayLatestData initLatestData(GpsHourSource gpsHourSource, Map map,GpsDayLatestData latestData) {
        return GpsDayLatestData.builder()
                .carId(gpsHourSource.getCarId())
                .time(gpsHourSource.getTime())
                .gpsCount(1)
                .isNonLocal(0)
                .maxSatelliteNum(gpsHourSource.getSatellites())
                .build();
    }

    @Override
    public GpsDayTransfor calcTransforData(GpsHourSource latestFirstData, GpsDayLatestData latestData, GpsHourSource gpsHourSource, Map map) {
        return GpsDayTransfor.builder()
                .carId(latestFirstData.getCarId())
                .time(latestFirstData.getTime())
                .gpsCount(latestData.getGpsCount())
                .isNonLocal(latestData.getIsNonLocal())
                .maxSatelliteNum(latestData.getMaxSatelliteNum())
                .build();
    }

    @Override
    public GpsDayTransfor initFromTempTransfer(GpsDayLatestData latestData, GpsHourSource gpsHourSource,Map map,long supplyTime) {
        return GpsDayTransfor.builder()
                .carId(latestData.getCarId())
                .maxSatelliteNum(0)
                .isNonLocal(0)
                .time(supplyTime)
                .gpsCount(0)
                .build();
    }

    @Override
    public Map<String, String> convertData2Map(GpsDayTransfor transfor,GpsDayLatestData latestData) {
        return null;
    }

    @Override
    public GpsDayTransfor calcIntegrityData(GpsHourSource latestFirstData, GpsDayLatestData latestData,GpsDayTransfor gpsDayTransfor, Map map) {
        return GpsDayTransfor.builder().maxSatelliteNum(latestData.getMaxSatelliteNum()).isNonLocal(latestData.getIsNonLocal())
                .gpsCount(latestData.getGpsCount()).carId(latestData.getCarId()).time(latestData.getTime()).build();
    }

    private GpsDayTransfor initTransfor() {
        return GpsDayTransfor.builder()
                .maxSatelliteNum(0)
                .isNonLocal(0)
                .gpsCount(0)
                .build();
    }

    @Override
    public GpsDayTransfor initFromDormancy(GpsDayLatestData latestData,long supplyTime) {
        return initFromTempTransfer(latestData,null,null,supplyTime);
    }
}

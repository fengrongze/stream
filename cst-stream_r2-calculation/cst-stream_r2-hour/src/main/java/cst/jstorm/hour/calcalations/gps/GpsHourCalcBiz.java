package cst.jstorm.hour.calcalations.gps;

import com.cst.stream.stathour.CSTData;
import com.cst.stream.stathour.gps.GpsHourLatestData;
import com.cst.stream.stathour.gps.GpsHourSource;
import com.cst.stream.stathour.gps.GpsHourTransfor;
import com.cst.jstorm.commons.stream.constants.ExceptionCodeStatus;
import com.cst.jstorm.commons.stream.operations.DataDealTransforInterface;
import com.cst.jstorm.commons.stream.operations.DataTransforInterface;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

import static com.cst.jstorm.commons.stream.operations.InvalidDataCharge.invalidTooLessThanTime;

/**
 * @author Johnney.chiu
 * create on 2017/12/20 16:18
 * @Description gps小时数据计算
 */
@NoArgsConstructor
@Slf4j
public class GpsHourCalcBiz implements DataTransforInterface<GpsHourTransfor,GpsHourSource>,
        DataDealTransforInterface<GpsHourTransfor,GpsHourSource,GpsHourLatestData> {

    @Override
    public void execute(GpsHourTransfor gpsHourTransfor,GpsHourSource gpsHourSource,Map ohter) {
        log.debug("gps before transfor hour data:{} {} {}",gpsHourSource.getCarId(),gpsHourSource.getTime(),gpsHourSource.toString());

        addGpsCount(gpsHourTransfor)
                .maxSatellitesCalc(gpsHourTransfor,gpsHourSource.getSatellites())
                .timeSetting(gpsHourTransfor,gpsHourSource.getTime());
        log.debug("gps before transfor hour data:{} {} {}", gpsHourTransfor.getCarId(),gpsHourTransfor.getTime(),gpsHourTransfor.toString());

    }

    @Override
    public GpsHourTransfor init(GpsHourSource gpsHourSource, Map<String,Object> other) {
        return new GpsHourTransfor(gpsHourSource.getCarId(), (Long)other.get("uploadTime"),0, 1,
                 0);
    }

    @Override
    public GpsHourTransfor initOffet(GpsHourTransfor gpsHourTransfor, GpsHourSource gpsHourSource
            , Map<String,Object> other) {

        return new GpsHourTransfor(gpsHourSource.getCarId(),(Long)other.get("uploadTime"),0, 1,
                 0);
    }

    @Override
    public GpsHourTransfor initFromTransfer(GpsHourTransfor gpsHourTransfor, Map<String, Object> other) {
        return new GpsHourTransfor(gpsHourTransfor.getCarId(),(Long)other.get("uploadTime"),0, 1,
                 0);
    }

    private GpsHourCalcBiz addGpsCount(GpsHourTransfor gpsHourTransfor) {
        gpsHourTransfor.setGpsCount(gpsHourTransfor.getGpsCount()+1);
        return this;
    }


    private GpsHourCalcBiz maxSatellitesCalc(GpsHourTransfor gpsHourTransfor,Integer satellites) {
        if(satellites>gpsHourTransfor.getMaxSatelliteNum()) {
            gpsHourTransfor.setMaxSatelliteNum(satellites);
        }
        return this;
    }

    private GpsHourCalcBiz timeSetting(GpsHourTransfor gpsHourTransfor,Long time) {
        gpsHourTransfor.setTime(time);
        return this;
    }

    @Override
    public ExceptionCodeStatus commpareExceptionWithEachData(GpsHourSource gpsHourSource, GpsHourLatestData latestData, Map map) {
        if (gpsHourSource.getTime() < latestData.getTime()) {
            log.warn("upload gps time exception,original data {},latest data :{}",gpsHourSource,latestData);
            return ExceptionCodeStatus.CALC_NO_TIME;
        }
        if (invalidTooLessThanTime(gpsHourSource.getTime())) {
            log.warn("upload gps time too less than now:original data {},latest data {}", gpsHourSource, latestData);
            return ExceptionCodeStatus.CALC_RETURN;
        }

        return ExceptionCodeStatus.CALC_DATA;
    }

    @Override
    public GpsHourLatestData calcLatestData(GpsHourLatestData latestData, GpsHourSource gpsHourSource, Map map,ExceptionCodeStatus status) {
        latestData.setCarId(gpsHourSource.getCarId());
        if(status!=ExceptionCodeStatus.CALC_NO_TIME)
            latestData.setTime(gpsHourSource.getTime());
        latestData.setGpsCount(latestData.getGpsCount()+1);
        latestData.setIsNonLocal(0);
        latestData.setMaxSatelliteNum(Math.max(latestData.getMaxSatelliteNum(),gpsHourSource.getSatellites()));
        return latestData;
    }

    @Override
    public GpsHourLatestData initLatestData(GpsHourSource gpsHourSource, Map map,GpsHourLatestData latestData) {
        return GpsHourLatestData.builder()
                .carId(gpsHourSource.getCarId())
                .time(gpsHourSource.getTime())
                .gpsCount(1)
                .isNonLocal(0)
                .maxSatelliteNum(gpsHourSource.getSatellites())
                .build();
    }

    @Override
    public GpsHourTransfor calcTransforData(GpsHourSource latestFirstData, GpsHourLatestData latestData, GpsHourSource gpsHourSource, Map map) {
        return GpsHourTransfor.builder()
                .carId(latestFirstData.getCarId())
                .time(latestFirstData.getTime())
                .gpsCount(latestData.getGpsCount())
                .isNonLocal(latestData.getIsNonLocal())
                .maxSatelliteNum(latestData.getMaxSatelliteNum())
                .build();
    }

    @Override
    public GpsHourTransfor initFromTempTransfer(GpsHourLatestData latestData,GpsHourSource gpsHourSource ,Map map,long supplyTime) {
        return GpsHourTransfor.builder()
                .carId(latestData.getCarId())
                .maxSatelliteNum(0)
                .isNonLocal(0)
                .time(supplyTime)
                .gpsCount(0)
                .build();
    }

    @Override
    public Map<String, String> convertData2Map(GpsHourTransfor transfor,GpsHourLatestData latestData) {
        return null;
    }

    @Override
    public GpsHourTransfor calcIntegrityData(GpsHourSource latestFirstData, GpsHourLatestData latestData,GpsHourTransfor gpsHourTransfor,Map map) {
        return null;
    }

    @Override
    public GpsHourTransfor initFromDormancy(GpsHourLatestData latestData, long l1) {
        return null;
    }
}

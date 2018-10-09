package cst.jstorm.daymonth.calcalations.gps;

import com.cst.stream.stathour.gps.GpsDayTransfor;
import com.cst.stream.stathour.gps.GpsYearTransfor;
import com.cst.jstorm.commons.stream.operations.DataAccumulationTransforInterface;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author Johnney.chiu
 * create on 2017/12/20 16:21
 * @Description gps 天数据计算
 */

@NoArgsConstructor
public class GpsYearCalcBiz implements DataAccumulationTransforInterface<GpsYearTransfor, GpsDayTransfor> {
    private final static Logger logger = LoggerFactory.getLogger(GpsYearCalcBiz.class);

    @Override
    public GpsYearTransfor initTransforDataBySource(GpsDayTransfor gpsDayTransfor, Map map) {
        GpsYearTransfor gpsYearTransfor = GpsYearTransfor.builder()
                .carId(gpsDayTransfor.getCarId())
                .gpsCount(gpsDayTransfor.getGpsCount())
                .nonLocalNum(gpsDayTransfor.getIsNonLocal())
                .time(gpsDayTransfor.getTime())
                .maxSatelliteNum(gpsDayTransfor.getMaxSatelliteNum())
                .build();
        return gpsYearTransfor;
    }

    @Override
    public GpsYearTransfor initTransforDataByLatest(GpsYearTransfor latestTransforData, Map map, Long time) {
        GpsYearTransfor gpsYearTransfor = GpsYearTransfor.builder()
                .carId(latestTransforData.getCarId())
                .gpsCount(0)
                .nonLocalNum(0)
                .time(time)
                .maxSatelliteNum(0)
                .build();
        return gpsYearTransfor;
    }

    @Override
    public GpsYearTransfor calcTransforData(GpsYearTransfor latestTransforData, GpsDayTransfor source, Map map) {
        GpsYearTransfor gpsYearTransfor = GpsYearTransfor.builder()
                .carId(latestTransforData.getCarId())
                .gpsCount(latestTransforData.getGpsCount()+source.getGpsCount())
                .time(source.getTime())
                .build();
        if(latestTransforData.getNonLocalNum()!=null){
            if(1==source.getIsNonLocal())
                gpsYearTransfor.setNonLocalNum(latestTransforData.getNonLocalNum()+1);
            else
                gpsYearTransfor.setNonLocalNum(latestTransforData.getNonLocalNum());
        }else{
            if (1 == source.getIsNonLocal()) {
                gpsYearTransfor.setNonLocalNum(1);
            } else {
                gpsYearTransfor.setNonLocalNum(0);
            }
        }
        if (latestTransforData.getMaxSatelliteNum() == null) {
            gpsYearTransfor.setMaxSatelliteNum(source.getMaxSatelliteNum());
        } else if (source.getMaxSatelliteNum() != null) {
            gpsYearTransfor.setMaxSatelliteNum(Math.max(latestTransforData.getMaxSatelliteNum(),source.getMaxSatelliteNum()));
        }

        return gpsYearTransfor;
    }
}

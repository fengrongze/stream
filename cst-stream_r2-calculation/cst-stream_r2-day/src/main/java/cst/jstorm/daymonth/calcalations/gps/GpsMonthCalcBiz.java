package cst.jstorm.daymonth.calcalations.gps;

import com.cst.stream.stathour.gps.GpsDayTransfor;
import com.cst.stream.stathour.gps.GpsMonthTransfor;
import com.cst.jstorm.commons.stream.operations.DataAccumulationTransforInterface;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author Johnney.chiu
 * create on 2017/12/20 16:21
 * @Description gps 月数据计算
 */

@NoArgsConstructor
public class GpsMonthCalcBiz implements DataAccumulationTransforInterface<GpsMonthTransfor, GpsDayTransfor> {
    private final static Logger logger = LoggerFactory.getLogger(GpsMonthCalcBiz.class);

    @Override
    public GpsMonthTransfor initTransforDataBySource(GpsDayTransfor gpsDayTransfor, Map map) {
        GpsMonthTransfor gpsMonthTransfor = GpsMonthTransfor.builder()
                .carId(gpsDayTransfor.getCarId())
                .gpsCount(gpsDayTransfor.getGpsCount())
                .nonLocalNum(gpsDayTransfor.getIsNonLocal())
                .time(gpsDayTransfor.getTime())
                .maxSatelliteNum(gpsDayTransfor.getMaxSatelliteNum())
                .build();
        return gpsMonthTransfor;
    }

    @Override
    public GpsMonthTransfor initTransforDataByLatest(GpsMonthTransfor latestTransforData, Map map, Long time) {
        GpsMonthTransfor gpsMonthTransfor = GpsMonthTransfor.builder()
                .carId(latestTransforData.getCarId())
                .gpsCount(0)
                .nonLocalNum(0)
                .time(time)
                .maxSatelliteNum(0)
                .build();
        return gpsMonthTransfor;
    }

    @Override
    public GpsMonthTransfor calcTransforData(GpsMonthTransfor latestTransforData, GpsDayTransfor source, Map map) {
        GpsMonthTransfor gpsMonthTransfor = GpsMonthTransfor.builder()
                .carId(latestTransforData.getCarId())
                .gpsCount(latestTransforData.getGpsCount()+source.getGpsCount())
                .time(source.getTime())
                .build();
        if(latestTransforData.getNonLocalNum()!=null){
            if(1==source.getIsNonLocal())
                gpsMonthTransfor.setNonLocalNum(latestTransforData.getNonLocalNum()+1);
            else
                gpsMonthTransfor.setNonLocalNum(latestTransforData.getNonLocalNum());
        }else{
            if (1 == source.getIsNonLocal()) {
                gpsMonthTransfor.setNonLocalNum(1);
            } else {
                gpsMonthTransfor.setNonLocalNum(0);
            }
        }
        if (latestTransforData.getMaxSatelliteNum() == null) {
            gpsMonthTransfor.setMaxSatelliteNum(source.getMaxSatelliteNum());
        } else if (source.getMaxSatelliteNum() != null) {
            gpsMonthTransfor.setMaxSatelliteNum(Math.max(latestTransforData.getMaxSatelliteNum(),source.getMaxSatelliteNum()));
        }

        return gpsMonthTransfor;
    }
}

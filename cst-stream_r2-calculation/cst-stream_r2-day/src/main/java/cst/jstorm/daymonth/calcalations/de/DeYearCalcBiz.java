package cst.jstorm.daymonth.calcalations.de;

import com.cst.stream.stathour.de.DeDayTransfor;
import com.cst.stream.stathour.de.DeYearTransfor;
import com.cst.jstorm.commons.stream.operations.DataAccumulationTransforInterface;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author Johnney.chiu
 * create on 2017/12/20 14:32
 * @Description de 年数据计算
 */
@NoArgsConstructor
public class DeYearCalcBiz implements DataAccumulationTransforInterface<DeYearTransfor, DeDayTransfor> {
    private final static Logger logger = LoggerFactory.getLogger(DeYearCalcBiz.class);

    @Override
    public DeYearTransfor initTransforDataBySource(DeDayTransfor deDayTransfor, Map map) {
        DeYearTransfor deYearTransfor = DeYearTransfor.builder()
                .carId(deDayTransfor.getCarId())
                .time(deDayTransfor.getTime())
                .rapidAccelerationCount(deDayTransfor.getRapidAccelerationCount())
                .rapidDecelerationCount(deDayTransfor.getRapidDecelerationCount())
                .sharpTurnCount(deDayTransfor.getSharpTurnCount())
                .build();
        return deYearTransfor;
    }

    @Override
    public DeYearTransfor initTransforDataByLatest(DeYearTransfor latestTransforData, Map map, Long time) {
        DeYearTransfor deYearTransfor = DeYearTransfor.builder()
                .carId(latestTransforData.getCarId())
                .time(time)
                .rapidAccelerationCount(0)
                .rapidDecelerationCount(0)
                .sharpTurnCount(0)
                .build();
        return deYearTransfor;
    }

    @Override
    public DeYearTransfor calcTransforData(DeYearTransfor latestTransforData, DeDayTransfor source, Map map) {
        DeYearTransfor deYearTransfor = DeYearTransfor.builder()
                .carId(source.getCarId())
                .time(source.getTime())
                .rapidAccelerationCount(latestTransforData.getRapidAccelerationCount()+source.getRapidAccelerationCount())
                .rapidDecelerationCount(latestTransforData.getRapidDecelerationCount()+source.getRapidDecelerationCount())
                .sharpTurnCount(latestTransforData.getSharpTurnCount()+source.getSharpTurnCount())
                .build();
        return deYearTransfor;
    }
}

package cst.jstorm.daymonth.calcalations.de;

import com.cst.stream.stathour.de.DeDayTransfor;
import com.cst.stream.stathour.de.DeMonthTransfor;
import com.cst.jstorm.commons.stream.operations.DataAccumulationTransforInterface;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author Johnney.chiu
 * create on 2017/12/20 14:32
 * @Description de 月数据计算
 */
@NoArgsConstructor
public class DeMonthCalcBiz implements DataAccumulationTransforInterface<DeMonthTransfor, DeDayTransfor> {
    private final static Logger logger = LoggerFactory.getLogger(DeMonthCalcBiz.class);

    @Override
    public DeMonthTransfor initTransforDataBySource(DeDayTransfor deDayTransfor, Map map) {
        DeMonthTransfor deMonthTransfor = DeMonthTransfor.builder()
                .carId(deDayTransfor.getCarId())
                .time(deDayTransfor.getTime())
                .rapidAccelerationCount(deDayTransfor.getRapidAccelerationCount())
                .rapidDecelerationCount(deDayTransfor.getRapidDecelerationCount())
                .sharpTurnCount(deDayTransfor.getSharpTurnCount())
                .build();
        return deMonthTransfor;
    }

    @Override
    public DeMonthTransfor initTransforDataByLatest(DeMonthTransfor latestTransforData, Map map, Long time) {
        DeMonthTransfor deMonthTransfor = DeMonthTransfor.builder()
                .carId(latestTransforData.getCarId())
                .time(time)
                .rapidAccelerationCount(0)
                .rapidDecelerationCount(0)
                .sharpTurnCount(0)
                .build();
        return deMonthTransfor;
    }

    @Override
    public DeMonthTransfor calcTransforData(DeMonthTransfor latestTransforData, DeDayTransfor source, Map map) {
        DeMonthTransfor deMonthTransfor = DeMonthTransfor.builder()
                .carId(source.getCarId())
                .time(source.getTime())
                .rapidAccelerationCount(latestTransforData.getRapidAccelerationCount()+source.getRapidAccelerationCount())
                .rapidDecelerationCount(latestTransforData.getRapidDecelerationCount()+source.getRapidDecelerationCount())
                .sharpTurnCount(latestTransforData.getSharpTurnCount()+source.getSharpTurnCount())
                .build();
        return deMonthTransfor;
    }
}

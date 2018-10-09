package cst.jstorm.daymonth.calcalations.am;

import com.cst.stream.stathour.am.AmDayTransfor;
import com.cst.stream.stathour.am.AmMonthTransfor;
import com.cst.jstorm.commons.stream.operations.DataAccumulationTransforInterface;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author Johnney.chiu
 * create on 2017/12/20 11:42
 * @Description Am 天数据计算
 */

@NoArgsConstructor
public class AmMonthCalcBiz implements DataAccumulationTransforInterface<AmMonthTransfor, AmDayTransfor> {
    private final static Logger logger = LoggerFactory.getLogger(AmMonthCalcBiz.class);

    @Override
    public AmMonthTransfor initTransforDataBySource(AmDayTransfor amDayTransfor, Map map) {
        AmMonthTransfor amMonthTransfor = AmMonthTransfor.builder()
                .carId(amDayTransfor.getCarId())
                .time(amDayTransfor.getTime())
                .ignition(amDayTransfor.getIgnition())
                .flameOut(amDayTransfor.getFlameOut())
                .insertNum(amDayTransfor.getInsertNum())
                .collision(amDayTransfor.getCollision())
                .overSpeed(amDayTransfor.getOverSpeed())
                .missingNum(amDayTransfor.getIsMissing()==null?0:amDayTransfor.getIsMissing())
                .pulloutTimes(amDayTransfor.getPulloutTimes())
                .fatigueNum(amDayTransfor.getIsFatigue()==null?0:amDayTransfor.getIsFatigue())
                .pulloutCounts(amDayTransfor.getPulloutCounts())
                .build();
        return amMonthTransfor;
    }

    @Override
    public AmMonthTransfor initTransforDataByLatest(AmMonthTransfor latestTransforData, Map map, Long time) {
        AmMonthTransfor amMonthTransfor = AmMonthTransfor.builder()
                .carId(latestTransforData.getCarId())
                .time(time)
                .ignition(0)
                .flameOut(0)
                .insertNum(0)
                .collision(0)
                .overSpeed(0)
                .missingNum(0)
                .pulloutTimes(0f)
                .fatigueNum(0)
                .pulloutCounts(0)
                .build();
        return amMonthTransfor;
    }

    @Override
    public AmMonthTransfor calcTransforData(AmMonthTransfor latestTransforData, AmDayTransfor source, Map map) {
        AmMonthTransfor amMonthTransfor = AmMonthTransfor.builder()
                .carId(source.getCarId())
                .time(source.getTime())
                .ignition(latestTransforData.getIgnition() + source.getIgnition())
                .flameOut(latestTransforData.getFlameOut() + source.getFlameOut())
                .insertNum(latestTransforData.getInsertNum() + source.getInsertNum())
                .collision(latestTransforData.getCollision() + source.getCollision())
                .overSpeed(latestTransforData.getOverSpeed() + source.getOverSpeed())
                .pulloutTimes(latestTransforData.getPulloutTimes() + source.getPulloutTimes())
                .pulloutCounts(latestTransforData.getPulloutCounts()+source.getPulloutCounts())
                .build();

        if (latestTransforData.getMissingNum() != null) {
            if(1==source.getIsMissing())
                amMonthTransfor.setMissingNum(latestTransforData.getMissingNum()+1);
            else
                amMonthTransfor.setMissingNum(latestTransforData.getMissingNum());
        }else{
            if (1 == source.getIsMissing()) {
                amMonthTransfor.setMissingNum(1);
            } else {
                amMonthTransfor.setMissingNum(0);
            }
        }

        if (latestTransforData.getFatigueNum() != null) {
            if(1==source.getIsFatigue())
                amMonthTransfor.setFatigueNum(latestTransforData.getFatigueNum()+1);
            else
                amMonthTransfor.setFatigueNum(latestTransforData.getFatigueNum());

        }else{
            if (1 == source.getIsFatigue()) {
                amMonthTransfor.setFatigueNum(1);
            } else {
                amMonthTransfor.setFatigueNum(0);
            }
        }
        return amMonthTransfor;
    }
}

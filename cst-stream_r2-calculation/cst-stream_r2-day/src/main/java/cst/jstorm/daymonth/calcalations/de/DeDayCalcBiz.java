package cst.jstorm.daymonth.calcalations.de;

import com.cst.stream.stathour.CSTData;
import com.cst.stream.stathour.de.DeDayLatestData;
import com.cst.stream.stathour.de.DeDayTransfor;
import com.cst.stream.stathour.de.DeHourSource;
import com.cst.stream.stathour.de.DeHourTransfor;
import com.cst.jstorm.commons.stream.constants.ExceptionCodeStatus;
import com.cst.jstorm.commons.stream.operations.DataDealTransforInterface;
import com.cst.jstorm.commons.stream.operations.DataTransforInterface;
import com.cst.stream.stathour.dormancy.DormancySource;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Johnney.chiu
 * create on 2017/12/20 14:29
 * @Description de 天计算
 */
@NoArgsConstructor
@Slf4j
public class DeDayCalcBiz implements
        DataDealTransforInterface<DeDayTransfor,DeHourSource,DeDayLatestData> {

    private DeDayCalcBiz addRapidAccelerationCount(DeHourTransfor deHourTransfor, Integer gatherType, Integer actType){
        if(3!=gatherType&&1==actType)
            deHourTransfor.setRapidAccelerationCount(deHourTransfor.getRapidAccelerationCount()+1);
        return this;
    }

    private DeDayCalcBiz addRapidDecelerationCount(DeHourTransfor deHourTransfor, Integer gatherType, Integer actType) {
        if(3!=gatherType&&2==actType)
            deHourTransfor.setRapidDecelerationCount(deHourTransfor.getRapidDecelerationCount()+1);
        return this;
    }
    private DeDayCalcBiz addSharpTurnCount(DeHourTransfor deHourTransfor, Integer gatherType, Integer actType){
        if(3!=gatherType&&3==actType)
            deHourTransfor.setSharpTurnCount(deHourTransfor.getSharpTurnCount()+1);
        return this;
    }

    @Override
    public ExceptionCodeStatus commpareExceptionWithEachData(DeHourSource deHourSource, DeDayLatestData latestData, Map map) {
        if (deHourSource.getTime() <= latestData.getTime()) {
            log.warn("upload de time exception,original data {},latest data :{}",deHourSource,latestData);
            return ExceptionCodeStatus.CALC_RETURN;
        }
        return ExceptionCodeStatus.CALC_DATA;
    }

    @Override
    public DeDayLatestData calcLatestData(DeDayLatestData latestData, DeHourSource deHourSource, Map map,ExceptionCodeStatus status) {
        latestData.setActType(deHourSource.getActType());
        if(status!=ExceptionCodeStatus.CALC_NO_TIME)
            latestData.setTime(deHourSource.getTime());
        latestData.setGatherType(deHourSource.getGatherType());
        latestData.setRapidAccelerationCounts(deHourSource.getRapidAccelerationCounts());
        latestData.setRapidDecelerationCounts(deHourSource.getRapidDecelerationCounts());
        latestData.setSharpTurnCounts(deHourSource.getSharpTurnCounts());
        latestData.setSpeed(deHourSource.getSpeed());
        return latestData;
    }

    @Override
    public DeDayLatestData initLatestData(DeHourSource deHourSource, Map map,DeDayLatestData latestData) {
        return DeDayLatestData.builder()
                .actType(deHourSource.getActType())
                .carId(deHourSource.getCarId())
                .gatherType(deHourSource.getGatherType())
                .rapidAccelerationCounts(deHourSource.getRapidAccelerationCounts())
                .rapidDecelerationCounts(deHourSource.getRapidDecelerationCounts())
                .sharpTurnCounts(deHourSource.getSharpTurnCounts())
                .speed(deHourSource.getSpeed())
                .time(deHourSource.getTime())
                .build();
    }

    @Override
    public DeDayTransfor calcTransforData(DeHourSource latestFirstData, DeDayLatestData latestData, DeHourSource deHourSource, Map map) {
        int rapidAccelerationCounts = deHourSource.getRapidAccelerationCounts() - latestFirstData.getRapidAccelerationCounts();
        int rapidDecelerationCounts = deHourSource.getRapidDecelerationCounts() - latestFirstData.getRapidDecelerationCounts();
        int sharpTurnCounts = deHourSource.getSharpTurnCounts() - latestFirstData.getSharpTurnCounts();
        return DeDayTransfor.builder()
                .carId(latestFirstData.getCarId())
                .time(latestFirstData.getTime())
                .rapidAccelerationCount(rapidAccelerationCounts > 0 ? rapidAccelerationCounts : 0)
                .rapidDecelerationCount(rapidDecelerationCounts > 0 ? rapidDecelerationCounts : 0)
                .sharpTurnCount(sharpTurnCounts > 0 ? sharpTurnCounts : 0)
                .build();
    }

    @Override
    public DeDayTransfor initFromTempTransfer(DeDayLatestData latestData,DeHourSource deHourSource, Map map,long supplyTime) {
        return DeDayTransfor.builder()
                .carId(latestData.getCarId())
                .sharpTurnCount(0)
                .rapidAccelerationCount(0)
                .rapidDecelerationCount(0)
                .time(supplyTime)
                .build();

    }

    @Override
    public Map<String, String> convertData2Map(DeDayTransfor transfor,DeDayLatestData latestData) {
        Map<String,String> map = null;
        if(null==transfor)
            transfor = initTransfor();
            map = new HashMap<>();
            map.put("carId",transfor.getCarId());
            map.put("rapidAccelerationCount",String.valueOf(transfor.getRapidAccelerationCount()));
            map.put("rapidDecelerationCount",String.valueOf(transfor.getRapidDecelerationCount()));
            map.put("sharpTurnCount",String.valueOf(transfor.getSharpTurnCount()));
        return map;
    }

    private DeDayTransfor initTransfor() {
        return DeDayTransfor.builder()
                .carId(null)
                .sharpTurnCount(0)
                .rapidAccelerationCount(0)
                .rapidDecelerationCount(0)
                .time(null)
                .build();
    }

    @Override
    public DeDayTransfor calcIntegrityData(DeHourSource latestFirstData, DeDayLatestData latestData,DeDayTransfor deDayTransfor, Map map) {
        int rapidAccelerationCounts = latestData.getRapidAccelerationCounts() - latestFirstData.getRapidAccelerationCounts();
        int rapidDecelerationCounts = latestData.getRapidDecelerationCounts() - latestFirstData.getRapidDecelerationCounts();
        int sharpTurnCounts = latestData.getSharpTurnCounts() - latestFirstData.getSharpTurnCounts();
        return DeDayTransfor.builder()
                .carId(latestFirstData.getCarId())
                .time(latestFirstData.getTime())
                .rapidAccelerationCount(rapidAccelerationCounts > 0 ? rapidAccelerationCounts : 0)
                .rapidDecelerationCount(rapidDecelerationCounts > 0 ? rapidDecelerationCounts : 0)
                .sharpTurnCount(sharpTurnCounts > 0 ? sharpTurnCounts : 0)
                .build();
    }

    @Override
    public DeDayTransfor initFromDormancy(DeDayLatestData latestData,long supplyTime) {
        return initFromTempTransfer(latestData,null,null,supplyTime);
    }
}

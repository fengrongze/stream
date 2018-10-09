package cst.jstorm.hour.calcalations.de;

import com.cst.stream.stathour.CSTData;
import com.cst.stream.stathour.de.DeDayTransfor;
import com.cst.stream.stathour.de.DeHourLatestData;
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
 * @Description de 小时数据计算
 */
@NoArgsConstructor
@Slf4j
public class DeHourCalcBiz implements DataTransforInterface<DeHourTransfor,DeHourSource>,
        DataDealTransforInterface<DeHourTransfor,DeHourSource,DeHourLatestData> {
    @Override
    public void execute(DeHourTransfor deHourTransfor,DeHourSource deHourSource, Map other) {
        log.debug("de before transfor hour data:{} {} {}",deHourSource.getCarId(),deHourSource.getTime(),deHourSource.toString());

        addRapidAccelerationCount(deHourTransfor,deHourSource.getGatherType(), deHourSource.getActType())
                .addRapidDecelerationCount(deHourTransfor,deHourSource.getGatherType(), deHourSource.getActType())
                .addSharpTurnCount(deHourTransfor,deHourSource.getGatherType(), deHourSource.getActType());
        log.debug("de after transfor hour data:{} {} {}", deHourTransfor.getCarId(),deHourTransfor.getTime(),deHourTransfor.toString());

    }

    @Override
    public DeHourTransfor init(DeHourSource deHourSource,  Map<String,Object> other) {
        return new DeHourTransfor(deHourSource.getCarId(), (Long)other.get("uploadTime"),
                0, 0, 0);
    }

    @Override
    public DeHourTransfor initOffet(DeHourTransfor deHourTransfor, DeHourSource deHourSource, Map<String,Object> other) {
         return init(deHourSource,other);
    }

    @Override
    public DeHourTransfor initFromTransfer(DeHourTransfor deHourTransfor, Map<String, Object> other) {
        return new DeHourTransfor(deHourTransfor.getCarId(), (Long)other.get("uploadTime"),
                0, 0, 0);
    }



    private DeHourCalcBiz addRapidAccelerationCount(DeHourTransfor deHourTransfor, Integer gatherType, Integer actType){
        if(3!=gatherType&&1==actType)
            deHourTransfor.setRapidAccelerationCount(deHourTransfor.getRapidAccelerationCount()+1);
        return this;
    }

    private DeHourCalcBiz  addRapidDecelerationCount(DeHourTransfor deHourTransfor,Integer gatherType, Integer actType) {
        if(3!=gatherType&&2==actType)
            deHourTransfor.setRapidDecelerationCount(deHourTransfor.getRapidDecelerationCount()+1);
        return this;
    }
    private DeHourCalcBiz addSharpTurnCount(DeHourTransfor deHourTransfor,Integer gatherType, Integer actType){
        if(3!=gatherType&&3==actType)
            deHourTransfor.setSharpTurnCount(deHourTransfor.getSharpTurnCount()+1);
        return this;
    }

    @Override
    public ExceptionCodeStatus commpareExceptionWithEachData(DeHourSource deHourSource, DeHourLatestData latestData, Map map) {
        if (deHourSource.getTime() <= latestData.getTime()) {
            log.warn("upload de time exception,original data {},latest data :{}",deHourSource,latestData);
            return ExceptionCodeStatus.CALC_RETURN;
        }
        return ExceptionCodeStatus.CALC_DATA;
    }

    @Override
    public DeHourLatestData calcLatestData(DeHourLatestData latestData, DeHourSource deHourSource, Map map,ExceptionCodeStatus status) {
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
    public DeHourLatestData initLatestData(DeHourSource deHourSource, Map map,DeHourLatestData latestData) {
        return DeHourLatestData.builder()
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
    public DeHourTransfor calcTransforData(DeHourSource latestFirstData, DeHourLatestData latestData, DeHourSource deHourSource, Map map) {
        int rapidAccelerationCounts = deHourSource.getRapidAccelerationCounts() - latestFirstData.getRapidAccelerationCounts();
        int rapidDecelerationCounts = deHourSource.getRapidDecelerationCounts() - latestFirstData.getRapidDecelerationCounts();
        int sharpTurnCounts = deHourSource.getSharpTurnCounts() - latestFirstData.getSharpTurnCounts();
        return DeHourTransfor.builder()
                .carId(latestFirstData.getCarId())
                .time(latestFirstData.getTime())
                .rapidAccelerationCount(rapidAccelerationCounts > 0 ? rapidAccelerationCounts : 0)
                .rapidDecelerationCount(rapidDecelerationCounts > 0 ? rapidDecelerationCounts : 0)
                .sharpTurnCount(sharpTurnCounts > 0 ? sharpTurnCounts : 0)
                .build();
    }

    @Override
    public DeHourTransfor initFromTempTransfer(DeHourLatestData latestData, DeHourSource deHourSource,Map map,long supplyTime) {
        return DeHourTransfor.builder()
                .carId(latestData.getCarId())
                .sharpTurnCount(0)
                .rapidAccelerationCount(0)
                .rapidDecelerationCount(0)
                .time(supplyTime)
                .build();

    }

    @Override
    public Map<String, String> convertData2Map(DeHourTransfor transfor,DeHourLatestData latestData) {
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

    private DeHourTransfor initTransfor() {
        return DeHourTransfor.builder()
                .carId(null)
                .sharpTurnCount(0)
                .rapidAccelerationCount(0)
                .rapidDecelerationCount(0)
                .time(null)
                .build();
    }

    @Override
    public DeHourTransfor calcIntegrityData(DeHourSource latestFirstData, DeHourLatestData latestData,DeHourTransfor deHourTransfor, Map map) {
        int rapidAccelerationCounts = latestData.getRapidAccelerationCounts() - latestFirstData.getRapidAccelerationCounts();
        int rapidDecelerationCounts = latestData.getRapidDecelerationCounts() - latestFirstData.getRapidDecelerationCounts();
        int sharpTurnCounts = latestData.getSharpTurnCounts() - latestFirstData.getSharpTurnCounts();
        return DeHourTransfor.builder()
                .carId(latestFirstData.getCarId())
                .time(latestFirstData.getTime())
                .rapidAccelerationCount(rapidAccelerationCounts > 0 ? rapidAccelerationCounts : 0)
                .rapidDecelerationCount(rapidDecelerationCounts > 0 ? rapidDecelerationCounts : 0)
                .sharpTurnCount(sharpTurnCounts > 0 ? sharpTurnCounts : 0)
                .build();
    }

    @Override
    public DeHourTransfor initFromDormancy(DeHourLatestData latestData, long l1) {
        return null;
    }
}

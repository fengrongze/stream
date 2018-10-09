package cst.jstorm.hour.calcalations.trace;

import com.cst.stream.stathour.CSTData;
import com.cst.stream.stathour.dormancy.DormancySource;
import com.cst.stream.stathour.trace.TraceHourLatestData;
import com.cst.stream.stathour.trace.TraceHourSource;
import com.cst.stream.stathour.trace.TraceHourTransfor;
import com.cst.jstorm.commons.stream.constants.ExceptionCodeStatus;
import com.cst.jstorm.commons.stream.operations.DataDealTransforInterface;
import com.cst.jstorm.commons.stream.operations.DataTransforInterface;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.util.HashSet;
import java.util.Map;

import static com.cst.jstorm.commons.stream.operations.InvalidDataCharge.invalidTooLessThanTime;

/**
 * @author Johnney.Chiu
 * create on 2018/4/13 17:39
 * @Description trace小时计算
 * @title
 */
@NoArgsConstructor
@Slf4j
public class TraceHourCalcBiz  implements DataTransforInterface<TraceHourTransfor,TraceHourSource>,
        DataDealTransforInterface<TraceHourTransfor,TraceHourSource,TraceHourLatestData> {
    @Override
    public void execute(TraceHourTransfor traceHourTransfor, TraceHourSource traceHourSource, Map<String, Object> other) {
        calcData(traceHourTransfor, traceHourSource);
    }

    @Override
    public TraceHourTransfor init(TraceHourSource traceHourSource, Map<String, Object> other) {
        return new TraceHourTransfor(traceHourSource.getCarId(), (Long)other.get("uploadTime"),
                1);
    }

    @Override
    public TraceHourTransfor initOffet(TraceHourTransfor traceHourTransfor, TraceHourSource traceHourSource, Map<String, Object> other) {
        return init(traceHourSource,other);
    }

    @Override
    public TraceHourTransfor initFromTransfer(TraceHourTransfor traceHourTransfor, Map<String, Object> other) {
        return new TraceHourTransfor(traceHourTransfor.getCarId(), (Long)other.get("uploadTime"),
                0);
    }

    private void calcData(TraceHourTransfor traceHourTransfor, TraceHourSource traceHourSource){
        if(StringUtils.isNotEmpty(traceHourSource.getTravelUuid())){
            traceHourTransfor.setTraceCounts(traceHourTransfor.getTraceCounts()+1);
            traceHourTransfor.setTime(traceHourSource.getTime());
            traceHourTransfor.setCarId(traceHourSource.getCarId());
        }
    }

    @Override
    public ExceptionCodeStatus commpareExceptionWithEachData(TraceHourSource traceHourSource, TraceHourLatestData latestData, Map map) {
        if (traceHourSource.getTime()< latestData.getTime()) {
            log.warn("upload trace time exception,original data {},latest data :{}",traceHourSource,latestData);
            return ExceptionCodeStatus.CALC_NO_TIME;
        }
        if (invalidTooLessThanTime(traceHourSource.getTime())) {
            log.warn("upload trace time too less than now:original data {},latest data {}", traceHourSource, latestData);
            return ExceptionCodeStatus.CALC_RETURN;
        }

        return ExceptionCodeStatus.CALC_DATA;
    }

    @Override
    public TraceHourLatestData calcLatestData(TraceHourLatestData latestData, TraceHourSource traceHourSource, Map map,ExceptionCodeStatus status) {
        latestData.setCarId(traceHourSource.getCarId());
        if(status!=ExceptionCodeStatus.CALC_NO_TIME)
            latestData.setTime(traceHourSource.getTime());
        if(traceHourSource.getTravelType()==1) {
            if(CollectionUtils.isNotEmpty(latestData.getTraces())){
                latestData.getTraces().add(traceHourSource.getTravelUuid());
            }
            else{
                latestData.setTraces(new HashSet<String>() {{
                    add(traceHourSource.getTravelUuid());
                }});
            }
        }

        return latestData;
    }

    @Override
    public TraceHourLatestData initLatestData(TraceHourSource traceHourSource, Map map,TraceHourLatestData latestData) {
        TraceHourLatestData traceHourLatestData= TraceHourLatestData.builder()
                .carId(traceHourSource.getCarId())
                .time(traceHourSource.getTime())
                .build();
        if (traceHourSource.getTravelType() == 1) {
            traceHourLatestData.setTraces(new HashSet<String>(){{add(traceHourSource.getTravelUuid());}});
        }

        return traceHourLatestData;
    }

    @Override
    public TraceHourTransfor calcTransforData(TraceHourSource latestFirstData, TraceHourLatestData latestData, TraceHourSource traceHourSource, Map map) {
        return TraceHourTransfor
                .builder()
                .carId(latestFirstData.getCarId())
                .time(latestFirstData.getTime())
                .traceCounts(CollectionUtils.isNotEmpty(latestData.getTraces())?latestData.getTraces().size():0)
                .build();
    }

    @Override
    public TraceHourTransfor initFromTempTransfer(TraceHourLatestData latestData,TraceHourSource traceHourSource,Map map,long supplyTime) {
        return TraceHourTransfor
                .builder()
                .carId(latestData.getCarId())
                .time(supplyTime)
                .traceCounts(0)
                .build();
    }

    @Override
    public Map<String, String> convertData2Map(TraceHourTransfor transfor,TraceHourLatestData latestData) {
        return null;
    }

    @Override
    public TraceHourTransfor calcIntegrityData(TraceHourSource latestFirstData, TraceHourLatestData latestData, TraceHourTransfor traceHourTransfor,Map map) {
        return null;
    }

    @Override
    public TraceHourTransfor initFromDormancy(TraceHourLatestData latestData, long l1) {
        return null;
    }
}

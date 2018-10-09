package cst.jstorm.daymonth.calcalations.trace;

import com.cst.stream.stathour.CSTData;
import com.cst.stream.stathour.trace.TraceDayLatestData;
import com.cst.stream.stathour.trace.TraceDayTransfor;
import com.cst.stream.stathour.trace.TraceHourSource;
import com.cst.stream.stathour.trace.TraceHourTransfor;
import com.cst.jstorm.commons.stream.constants.ExceptionCodeStatus;
import com.cst.jstorm.commons.stream.operations.DataDealTransforInterface;
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
 * @Description trace天计算
 * @title
 */
@NoArgsConstructor
@Slf4j
public class TraceDayCalcBiz implements
        DataDealTransforInterface<TraceDayTransfor,TraceHourSource,TraceDayLatestData> {

    private void calcData(TraceHourTransfor traceHourTransfor, TraceHourSource traceHourSource){
        if(StringUtils.isNotEmpty(traceHourSource.getTravelUuid())){
            traceHourTransfor.setTraceCounts(traceHourTransfor.getTraceCounts()+1);
            traceHourTransfor.setTime(traceHourSource.getTime());
            traceHourTransfor.setCarId(traceHourSource.getCarId());
        }
    }

    @Override
    public ExceptionCodeStatus commpareExceptionWithEachData(TraceHourSource traceHourSource, TraceDayLatestData latestData, Map map) {
        if (traceHourSource.getTime() < latestData.getTime()) {
            log.warn("upload trace time exception,original data {},latest data :{}",traceHourSource,latestData);
            return ExceptionCodeStatus.CALC_NO_TIME;
        }

        return ExceptionCodeStatus.CALC_DATA;
    }

    @Override
    public TraceDayLatestData calcLatestData(TraceDayLatestData latestData, TraceHourSource traceHourSource, Map map,ExceptionCodeStatus status) {
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
    public TraceDayLatestData initLatestData(TraceHourSource traceHourSource, Map map,TraceDayLatestData latestData) {
        TraceDayLatestData traceDayLatestData= TraceDayLatestData.builder()
                .carId(traceHourSource.getCarId())
                .time(traceHourSource.getTime())
                .build();

        if (traceHourSource.getTravelType() == 1) {
            traceDayLatestData.setTraces(new HashSet<String>(){{add(traceHourSource.getTravelUuid());}});
        }
        return traceDayLatestData;
    }

    @Override
    public TraceDayTransfor calcTransforData(TraceHourSource latestFirstData, TraceDayLatestData latestData, TraceHourSource traceHourSource, Map map) {
        return TraceDayTransfor.builder()
                .carId(latestFirstData.getCarId())
                .time(latestFirstData.getTime())
                .traceCounts(CollectionUtils.isNotEmpty(latestData.getTraces())?latestData.getTraces().size():0)
                .build();
    }

    @Override
    public TraceDayTransfor initFromTempTransfer(TraceDayLatestData latestData,TraceHourSource traceHourSource ,Map map,long supplyTime) {
        return TraceDayTransfor
                .builder()
                .carId(latestData.getCarId())
                .time(supplyTime)
                .traceCounts(0)
                .build();
    }

    @Override
    public Map<String, String> convertData2Map(TraceDayTransfor transfor,TraceDayLatestData latestData) {
        return null;
    }

    @Override
    public TraceDayTransfor calcIntegrityData(TraceHourSource latestFirstData, TraceDayLatestData latestData,TraceDayTransfor traceDayTransfor,Map map) {
        return TraceDayTransfor.builder()
                .carId(latestData.getCarId())
                .traceCounts(CollectionUtils.isNotEmpty(latestData.getTraces())?latestData.getTraces().size():0)
                .time(latestData.getTime())
                .build();
    }

    private TraceDayTransfor initTransfor(){
        return TraceDayTransfor.builder().traceCounts(0).build();
    }

    @Override
    public TraceDayTransfor initFromDormancy(TraceDayLatestData latestData, long supplyTime) {
        return initFromTempTransfer(latestData,null,null,supplyTime);
    }
}

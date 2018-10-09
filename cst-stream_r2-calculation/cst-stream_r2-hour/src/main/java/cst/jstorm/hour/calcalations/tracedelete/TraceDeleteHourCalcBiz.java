package cst.jstorm.hour.calcalations.tracedelete;

import com.cst.stream.stathour.CSTData;
import com.cst.stream.stathour.dormancy.DormancySource;
import com.cst.stream.stathour.tracedelete.TraceDeleteHourLatestData;
import com.cst.stream.stathour.tracedelete.TraceDeleteHourSource;
import com.cst.stream.stathour.tracedelete.TraceDeleteHourTransfor;
import com.cst.jstorm.commons.stream.constants.ExceptionCodeStatus;
import com.cst.jstorm.commons.stream.operations.DataDealTransforInterface;
import com.cst.jstorm.commons.stream.operations.DataTransforInterface;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

import static com.cst.jstorm.commons.stream.operations.InvalidDataCharge.invalidTooLessThanTime;

/**
 * @author Johnney.Chiu
 * create on 2018/4/13 17:39
 * @Description trace小时计算
 * @title
 */
@Slf4j
public class TraceDeleteHourCalcBiz implements DataTransforInterface<TraceDeleteHourTransfor,TraceDeleteHourSource>,
        DataDealTransforInterface<TraceDeleteHourTransfor,TraceDeleteHourSource,TraceDeleteHourLatestData> {
    @Override
    public void execute(TraceDeleteHourTransfor traceDeleteHourTransfor, TraceDeleteHourSource traceDeleteHourSource, Map<String, Object> other) {
        calcData(traceDeleteHourTransfor, traceDeleteHourSource, other);
    }

    @Override
    public TraceDeleteHourTransfor init(TraceDeleteHourSource traceDeleteHourSource, Map<String, Object> other) {
        return TraceDeleteHourTransfor.builder().carId(traceDeleteHourSource.getCarId())
                .time((Long) other.get("uploadTime"))
                .traceDeleteCounts(1)
                .build();
    }

    @Override
    public TraceDeleteHourTransfor initOffet(TraceDeleteHourTransfor traceDeleteHourTransfor, TraceDeleteHourSource traceDeleteHourSource, Map<String, Object> other) {
        return init(traceDeleteHourSource, other);
    }

    @Override
    public TraceDeleteHourTransfor initFromTransfer(TraceDeleteHourTransfor traceDeleteHourTransfor, Map<String, Object> other) {
        return TraceDeleteHourTransfor.builder().carId(traceDeleteHourTransfor.getCarId())
                .time(traceDeleteHourTransfor.getTime())
                .traceDeleteCounts(0)
                .build();

    }

    private void calcData(TraceDeleteHourTransfor traceDeleteHourTransfor, TraceDeleteHourSource traceDeleteHourSource, Map<String, Object> other) {
        traceDeleteHourTransfor.setTraceDeleteCounts(traceDeleteHourTransfor.getTraceDeleteCounts() + traceDeleteHourTransfor.getTraceDeleteCounts());
        traceDeleteHourTransfor.setCarId(traceDeleteHourSource.getCarId());
        traceDeleteHourTransfor.setTime(traceDeleteHourSource.getTime());
    }

    @Override
    public ExceptionCodeStatus commpareExceptionWithEachData(TraceDeleteHourSource traceDeleteHourSource, TraceDeleteHourLatestData latestData, Map map) {
        if (traceDeleteHourSource.getTime() <= latestData.getTime()) {
            log.warn("upload trace delete time exception,original data {},latest data :{}",traceDeleteHourSource,latestData);
            return ExceptionCodeStatus.CALC_NO_TIME;
        }
        if (invalidTooLessThanTime(traceDeleteHourSource.getTime())) {
            log.warn("upload trace delete time too less than now:original data {},latest data {}", traceDeleteHourSource, latestData);
            return ExceptionCodeStatus.CALC_RETURN;
        }
        return ExceptionCodeStatus.CALC_DATA;
    }

    @Override
    public TraceDeleteHourLatestData calcLatestData(TraceDeleteHourLatestData latestData, TraceDeleteHourSource traceDeleteHourSource, Map map, ExceptionCodeStatus status) {
        if(status!=ExceptionCodeStatus.CALC_NO_TIME)
            latestData.setTime(traceDeleteHourSource.getTime());
        latestData.setCarId(traceDeleteHourSource.getCarId());
        latestData.setTraceDeleteCounts(latestData.getTraceDeleteCounts() + 1);
        return latestData;
    }

    @Override
    public TraceDeleteHourLatestData initLatestData(TraceDeleteHourSource traceDeleteHourSource, Map map,TraceDeleteHourLatestData latestData) {
        return TraceDeleteHourLatestData.builder()
                .carId(traceDeleteHourSource.getCarId())
                .time(traceDeleteHourSource.getTime())
                .traceDeleteCounts(1)
                .build();
    }

    @Override
    public TraceDeleteHourTransfor calcTransforData(TraceDeleteHourSource latestFirstData, TraceDeleteHourLatestData latestData, TraceDeleteHourSource traceDeleteHourSource, Map map) {

        return TraceDeleteHourTransfor.builder().carId(latestFirstData.getCarId()).time(latestFirstData.getTime())
                .traceDeleteCounts(latestData.getTraceDeleteCounts()).build();
    }

    @Override
    public TraceDeleteHourTransfor initFromTempTransfer(TraceDeleteHourLatestData latestData,TraceDeleteHourSource traceDeleteHourSource,Map map,long supplyTime) {
        return TraceDeleteHourTransfor.builder()
                .carId(latestData.getCarId())
                .time(supplyTime)
                .traceDeleteCounts(0)
                .build();
    }

    @Override
    public Map<String, String> convertData2Map(TraceDeleteHourTransfor transfor,TraceDeleteHourLatestData latestData) {
        return null;
    }

    @Override
    public TraceDeleteHourTransfor calcIntegrityData(TraceDeleteHourSource latestFirstData, TraceDeleteHourLatestData latestData,TraceDeleteHourTransfor traceDeleteHourTransfor,Map map) {
        return null;
    }

    @Override
    public TraceDeleteHourTransfor initFromDormancy(TraceDeleteHourLatestData latestData, long l1) {
        return null;
    }
}


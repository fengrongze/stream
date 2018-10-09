package cst.jstorm.daymonth.calcalations.tracedelete;

import com.cst.stream.stathour.CSTData;
import com.cst.stream.stathour.tracedelete.TraceDeleteDayLatestData;
import com.cst.stream.stathour.tracedelete.TraceDeleteDayTransfor;
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
 * @Description trace天计算
 * @title
 */
@Slf4j
public class TraceDeleteDayCalcBiz implements
        DataDealTransforInterface<TraceDeleteDayTransfor,TraceDeleteHourSource,TraceDeleteDayLatestData> {

    private void calcData(TraceDeleteHourTransfor traceDeleteHourTransfor, TraceDeleteHourSource traceDeleteHourSource, Map<String, Object> other) {
        traceDeleteHourTransfor.setTraceDeleteCounts(traceDeleteHourTransfor.getTraceDeleteCounts() + traceDeleteHourTransfor.getTraceDeleteCounts());
        traceDeleteHourTransfor.setCarId(traceDeleteHourSource.getCarId());
        traceDeleteHourTransfor.setTime(traceDeleteHourSource.getTime());
    }

    @Override
    public ExceptionCodeStatus commpareExceptionWithEachData(TraceDeleteHourSource traceDeleteHourSource, TraceDeleteDayLatestData latestData, Map map) {
        if (traceDeleteHourSource.getTime() < latestData.getTime()) {
            log.warn("upload trace delete exception,original data {},latest data :{}",traceDeleteHourSource,latestData);
            return ExceptionCodeStatus.CALC_NO_TIME;
        }

        return ExceptionCodeStatus.CALC_DATA;
    }

    @Override
    public TraceDeleteDayLatestData calcLatestData(TraceDeleteDayLatestData latestData, TraceDeleteHourSource traceDeleteHourSource, Map map, ExceptionCodeStatus status) {
        if(status!=ExceptionCodeStatus.CALC_NO_TIME)
            latestData.setTime(traceDeleteHourSource.getTime());
        latestData.setCarId(traceDeleteHourSource.getCarId());
        latestData.setTraceDeleteCounts(latestData.getTraceDeleteCounts() + 1);
        return latestData;
    }

    @Override
    public TraceDeleteDayLatestData initLatestData(TraceDeleteHourSource traceDeleteHourSource, Map map,TraceDeleteDayLatestData latestData) {
        return TraceDeleteDayLatestData.builder()
                .carId(traceDeleteHourSource.getCarId())
                .time(traceDeleteHourSource.getTime())
                .traceDeleteCounts(1)
                .build();
    }

    @Override
    public TraceDeleteDayTransfor calcTransforData(TraceDeleteHourSource latestFirstData, TraceDeleteDayLatestData latestData, TraceDeleteHourSource traceDeleteHourSource, Map map) {
        TraceDeleteDayTransfor traceDeleteDayTransfor = TraceDeleteDayTransfor.builder().carId(latestFirstData.getCarId()).time(latestFirstData.getTime())
                .traceDeleteCounts(latestData.getTraceDeleteCounts()).build();
        return traceDeleteDayTransfor;
    }

    @Override
    public TraceDeleteDayTransfor initFromTempTransfer(TraceDeleteDayLatestData latestData,TraceDeleteHourSource traceDeleteHourSource,Map map,long supplyTime) {
        return TraceDeleteDayTransfor.builder()
                .carId(latestData.getCarId())
                .time(supplyTime)
                .traceDeleteCounts(0)
                .build();
    }

    @Override
    public Map<String, String> convertData2Map(TraceDeleteDayTransfor transfor,TraceDeleteDayLatestData latestData) {
        return null;
    }

    @Override
    public TraceDeleteDayTransfor calcIntegrityData(TraceDeleteHourSource latestFirstData, TraceDeleteDayLatestData latestData,TraceDeleteDayTransfor traceDeleteDayTransfor,Map map) {
        return TraceDeleteDayTransfor.builder().traceDeleteCounts(latestData.getTraceDeleteCounts())
                .time(latestData.getTime()).carId(latestData.getCarId()).build();
    }
    private TraceDeleteDayTransfor initTransfor(){
        return TraceDeleteDayTransfor.builder().traceDeleteCounts(0).build();
    }

    @Override
    public TraceDeleteDayTransfor initFromDormancy(TraceDeleteDayLatestData latestData,long supplyTime) {
        return initFromTempTransfer(latestData,null,null,supplyTime);
    }
}


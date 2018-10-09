package cst.jstorm.daymonth.calcalations.tracedelete;

import com.cst.stream.stathour.tracedelete.TraceDeleteDayTransfor;
import com.cst.stream.stathour.tracedelete.TraceDeleteMonthTransfor;
import com.cst.jstorm.commons.stream.operations.DataAccumulationTransforInterface;

import java.util.Map;

/**
 * @author Johnney.Chiu
 * create on 2018/4/16 11:15
 * @Description 轨迹删除计算
 * @title
 */
public class TraceDeleteMonthCalcBiz implements DataAccumulationTransforInterface<TraceDeleteMonthTransfor,TraceDeleteDayTransfor> {
    @Override
    public TraceDeleteMonthTransfor initTransforDataBySource(TraceDeleteDayTransfor traceDeleteDayTransfor, Map map) {
        TraceDeleteMonthTransfor traceDeleteMonthTransfor = TraceDeleteMonthTransfor.builder()
                .carId(traceDeleteDayTransfor.getCarId())
                .time(traceDeleteDayTransfor.getTime())
                .traceDeleteCounts(traceDeleteDayTransfor.getTraceDeleteCounts())
                .build();
        return traceDeleteMonthTransfor;
    }

    @Override
    public TraceDeleteMonthTransfor initTransforDataByLatest(TraceDeleteMonthTransfor latestTransforData, Map map, Long time) {
        TraceDeleteMonthTransfor traceDeleteMonthTransfor = TraceDeleteMonthTransfor.builder()
                .carId(latestTransforData.getCarId())
                .time(time)
                .traceDeleteCounts(0)
                .build();
        return traceDeleteMonthTransfor;
    }

    @Override
    public TraceDeleteMonthTransfor calcTransforData(TraceDeleteMonthTransfor latestTransforData, TraceDeleteDayTransfor source, Map map) {
        TraceDeleteMonthTransfor traceDeleteMonthTransfor = TraceDeleteMonthTransfor.builder()
                .carId(latestTransforData.getCarId())
                .time(source.getTime())
                .traceDeleteCounts((latestTransforData.getTraceDeleteCounts()==null?0:latestTransforData.getTraceDeleteCounts())
                        +(source.getTraceDeleteCounts()==null?0:source.getTraceDeleteCounts()))
                .build();
        return traceDeleteMonthTransfor;
    }
}

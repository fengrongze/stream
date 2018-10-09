package cst.jstorm.daymonth.calcalations.tracedelete;

import com.cst.stream.stathour.tracedelete.TraceDeleteDayTransfor;
import com.cst.stream.stathour.tracedelete.TraceDeleteYearTransfor;
import com.cst.jstorm.commons.stream.operations.DataAccumulationTransforInterface;

import java.util.Map;

/**
 * @author Johnney.Chiu
 * create on 2018/4/16 11:15
 * @Description 轨迹删除计算
 * @title
 */
public class TraceDeleteYearCalcBiz implements DataAccumulationTransforInterface<TraceDeleteYearTransfor,TraceDeleteDayTransfor> {
    @Override
    public TraceDeleteYearTransfor initTransforDataBySource(TraceDeleteDayTransfor traceDeleteDayTransfor, Map map) {
        TraceDeleteYearTransfor traceDeleteYearTransfor = TraceDeleteYearTransfor.builder()
                .carId(traceDeleteDayTransfor.getCarId())
                .time(traceDeleteDayTransfor.getTime())
                .traceDeleteCounts(traceDeleteDayTransfor.getTraceDeleteCounts())
                .build();
        return traceDeleteYearTransfor;
    }

    @Override
    public TraceDeleteYearTransfor initTransforDataByLatest(TraceDeleteYearTransfor latestTransforData, Map map, Long time) {
        TraceDeleteYearTransfor traceDeleteYearTransfor = TraceDeleteYearTransfor.builder()
                .carId(latestTransforData.getCarId())
                .time(time)
                .traceDeleteCounts(0)
                .build();
        return traceDeleteYearTransfor;
    }

    @Override
    public TraceDeleteYearTransfor calcTransforData(TraceDeleteYearTransfor latestTransforData, TraceDeleteDayTransfor source, Map map) {
        TraceDeleteYearTransfor traceDeleteYearTransfor = TraceDeleteYearTransfor.builder()
                .carId(latestTransforData.getCarId())
                .time(source.getTime())
                .traceDeleteCounts((latestTransforData.getTraceDeleteCounts()==null?0:latestTransforData.getTraceDeleteCounts())
                        +(source.getTraceDeleteCounts()==null?0:source.getTraceDeleteCounts()))
                .build();
        return traceDeleteYearTransfor;
    }
}

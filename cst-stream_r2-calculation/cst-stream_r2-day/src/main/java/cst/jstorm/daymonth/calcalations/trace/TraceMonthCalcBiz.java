package cst.jstorm.daymonth.calcalations.trace;

import com.cst.stream.stathour.trace.TraceDayTransfor;
import com.cst.stream.stathour.trace.TraceMonthTransfor;
import com.cst.jstorm.commons.stream.operations.DataAccumulationTransforInterface;

import java.util.Map;

/**
 * @author Johnney.Chiu
 * create on 2018/4/16 11:14
 * @Description 轨迹月计算
 * @title
 */
public class TraceMonthCalcBiz implements DataAccumulationTransforInterface<TraceMonthTransfor,TraceDayTransfor> {
    @Override
    public TraceMonthTransfor initTransforDataBySource(TraceDayTransfor traceDayTransfor, Map map) {
        TraceMonthTransfor traceMonthTransfor = TraceMonthTransfor.builder()
                .carId(traceDayTransfor.getCarId())
                .time(traceDayTransfor.getTime())
                .traceCounts(traceDayTransfor.getTraceCounts())
                .build();
        return traceMonthTransfor;
    }

    @Override
    public TraceMonthTransfor initTransforDataByLatest(TraceMonthTransfor latestTransforData, Map map, Long time){
        TraceMonthTransfor traceMonthTransfor = TraceMonthTransfor.builder()
                .carId(latestTransforData.getCarId())
                .time(time)
                .traceCounts(0)
                .build();
        return traceMonthTransfor;
    }

    @Override
    public TraceMonthTransfor calcTransforData(TraceMonthTransfor latestTransforData, TraceDayTransfor source, Map map) {
        TraceMonthTransfor traceMonthTransfor = TraceMonthTransfor.builder()
                .carId(latestTransforData.getCarId())
                .time(source.getTime())
                .traceCounts((latestTransforData.getTraceCounts()==null?0:latestTransforData.getTraceCounts())
                        +(source.getTraceCounts()==null?0:source.getTraceCounts()))
                .build();
        return traceMonthTransfor;
    }
}

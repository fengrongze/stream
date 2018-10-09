package cst.jstorm.daymonth.calcalations.trace;

import com.cst.stream.stathour.trace.TraceDayTransfor;
import com.cst.stream.stathour.trace.TraceYearTransfor;
import com.cst.jstorm.commons.stream.operations.DataAccumulationTransforInterface;

import java.util.Map;

/**
 * @author Johnney.Chiu
 * create on 2018/4/16 11:14
 * @Description 轨迹年计算
 * @title
 */
public class TraceYearCalcBiz implements DataAccumulationTransforInterface<TraceYearTransfor,TraceDayTransfor> {
    @Override
    public TraceYearTransfor initTransforDataBySource(TraceDayTransfor traceDayTransfor, Map map) {
        TraceYearTransfor traceYearTransfor = TraceYearTransfor.builder()
                .carId(traceDayTransfor.getCarId())
                .time(traceDayTransfor.getTime())
                .traceCounts(traceDayTransfor.getTraceCounts())
                .build();
        return traceYearTransfor;
    }

    @Override
    public TraceYearTransfor initTransforDataByLatest(TraceYearTransfor latestTransforData, Map map, Long time){
        TraceYearTransfor traceYearTransfor = TraceYearTransfor.builder()
                .carId(latestTransforData.getCarId())
                .time(time)
                .traceCounts(0)
                .build();
        return traceYearTransfor;
    }

    @Override
    public TraceYearTransfor calcTransforData(TraceYearTransfor latestTransforData, TraceDayTransfor source, Map map) {
        TraceYearTransfor traceYearTransfor = TraceYearTransfor.builder()
                .carId(latestTransforData.getCarId())
                .time(source.getTime())
                .traceCounts((latestTransforData.getTraceCounts()==null?0:latestTransforData.getTraceCounts())
                        +(source.getTraceCounts()==null?0:source.getTraceCounts()))
                .build();
        return traceYearTransfor;
    }
}

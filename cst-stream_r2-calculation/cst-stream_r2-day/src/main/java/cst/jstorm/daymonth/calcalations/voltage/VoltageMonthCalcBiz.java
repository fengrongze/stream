package cst.jstorm.daymonth.calcalations.voltage;

import com.cst.stream.stathour.voltage.VoltageDayTransfor;
import com.cst.stream.stathour.voltage.VoltageMonthTransfor;
import com.cst.jstorm.commons.stream.operations.DataAccumulationTransforInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author Johnney.Chiu
 * create on 2018/4/16 11:15
 * @Description voltage 月计算
 * @title
 */

public class VoltageMonthCalcBiz implements DataAccumulationTransforInterface<VoltageMonthTransfor,VoltageDayTransfor> {
    Logger logger = LoggerFactory.getLogger(VoltageMonthCalcBiz.class);
    @Override
    public VoltageMonthTransfor initTransforDataBySource(VoltageDayTransfor voltageDayTransfor, Map map) {
        VoltageMonthTransfor voltageMonthTransfor = VoltageMonthTransfor.builder()
                .carId(voltageDayTransfor.getCarId())
                .time(voltageDayTransfor.getTime())
                .maxVoltage(voltageDayTransfor.getMaxVoltage())
                .minVoltage(voltageDayTransfor.getMinVoltage())
                .build();
        return voltageMonthTransfor;
    }

    @Override
    public VoltageMonthTransfor initTransforDataByLatest(VoltageMonthTransfor latestTransforData, Map map, Long time) {
        VoltageMonthTransfor voltageMonthTransfor = VoltageMonthTransfor.builder()
                .carId(latestTransforData.getCarId())
                .time(time)
                .maxVoltage(0F)
                .minVoltage(0F)
                .build();
        return voltageMonthTransfor;
    }

    @Override
    public VoltageMonthTransfor calcTransforData(VoltageMonthTransfor latestTransforData, VoltageDayTransfor source, Map map) {
        VoltageMonthTransfor voltageMonthTransfor = VoltageMonthTransfor.builder()
                .carId(latestTransforData.getCarId())
                .time(source.getTime())
                .build();
        if (latestTransforData.getMaxVoltage() == null) {
            voltageMonthTransfor.setMaxVoltage(source.getMaxVoltage()==null?0:source.getMaxVoltage());
        } else if(source.getMaxVoltage()!=null){
            voltageMonthTransfor.setMaxVoltage(Math.max(source.getMaxVoltage(), source.getMaxVoltage()));

        }
        if (latestTransforData.getMinVoltage() == null) {
            voltageMonthTransfor.setMinVoltage(source.getMinVoltage()==null?0:source.getMinVoltage());
        } else if(source.getMinVoltage()==null){
            voltageMonthTransfor.setMinVoltage(Math.min(source.getMinVoltage(), source.getMinVoltage()));
        }

        return voltageMonthTransfor;
    }
}

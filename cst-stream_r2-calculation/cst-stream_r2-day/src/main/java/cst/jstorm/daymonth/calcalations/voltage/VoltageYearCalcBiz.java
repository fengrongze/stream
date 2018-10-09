package cst.jstorm.daymonth.calcalations.voltage;

import com.cst.stream.stathour.voltage.VoltageDayTransfor;
import com.cst.stream.stathour.voltage.VoltageYearTransfor;
import com.cst.jstorm.commons.stream.operations.DataAccumulationTransforInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author Johnney.Chiu
 * create on 2018/4/16 11:15
 * @Description voltage 年计算
 * @title
 */

public class VoltageYearCalcBiz implements DataAccumulationTransforInterface<VoltageYearTransfor,VoltageDayTransfor> {
    Logger logger = LoggerFactory.getLogger(VoltageYearCalcBiz.class);
    @Override
    public VoltageYearTransfor initTransforDataBySource(VoltageDayTransfor voltageDayTransfor, Map map) {
        VoltageYearTransfor voltageYearTransfor = VoltageYearTransfor.builder()
                .carId(voltageDayTransfor.getCarId())
                .time(voltageDayTransfor.getTime())
                .maxVoltage(voltageDayTransfor.getMaxVoltage())
                .minVoltage(voltageDayTransfor.getMinVoltage())
                .build();
        return voltageYearTransfor;
    }

    @Override
    public VoltageYearTransfor initTransforDataByLatest(VoltageYearTransfor latestTransforData, Map map, Long time) {
        VoltageYearTransfor voltageYearTransfor = VoltageYearTransfor.builder()
                .carId(latestTransforData.getCarId())
                .time(time)
                .maxVoltage(0F)
                .minVoltage(0F)
                .build();
        return voltageYearTransfor;
    }

    @Override
    public VoltageYearTransfor calcTransforData(VoltageYearTransfor latestTransforData, VoltageDayTransfor source, Map map) {
        VoltageYearTransfor voltageYearTransfor = VoltageYearTransfor.builder()
                .carId(latestTransforData.getCarId())
                .time(source.getTime())
                .build();
        if (latestTransforData.getMaxVoltage() == null) {
            voltageYearTransfor.setMaxVoltage(source.getMaxVoltage()==null?0:source.getMaxVoltage());
        } else if(source.getMaxVoltage()!=null){
            voltageYearTransfor.setMaxVoltage(Math.max(source.getMaxVoltage(), source.getMaxVoltage()));

        }
        if (latestTransforData.getMinVoltage() == null) {
            voltageYearTransfor.setMinVoltage(source.getMinVoltage()==null?0:source.getMinVoltage());
        } else if(source.getMinVoltage()==null){
            voltageYearTransfor.setMinVoltage(Math.min(source.getMinVoltage(), source.getMinVoltage()));
        }

        return voltageYearTransfor;
    }
}

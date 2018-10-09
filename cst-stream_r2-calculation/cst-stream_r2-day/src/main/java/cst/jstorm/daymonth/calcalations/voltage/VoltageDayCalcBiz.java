package cst.jstorm.daymonth.calcalations.voltage;

import com.cst.stream.stathour.CSTData;
import com.cst.stream.stathour.voltage.*;
import com.cst.jstorm.commons.stream.constants.ExceptionCodeStatus;
import com.cst.jstorm.commons.stream.operations.DataDealTransforInterface;
import com.cst.jstorm.commons.stream.operations.DataTransforInterface;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.cst.jstorm.commons.stream.operations.InvalidDataCharge.invalidTooLessThanTime;

/**
 * @author Johnney.Chiu
 * create on 2018/4/13 17:39
 * @Description trace天计算
 * @title
 */
@Slf4j
public class VoltageDayCalcBiz implements
        DataDealTransforInterface<VoltageDayTransfor,VoltageHourSource,VoltageDayLatestData> {
    private final static Logger logger = LoggerFactory.getLogger(VoltageDayCalcBiz.class);

    private void calcData(VoltageHourTransfor voltageHourTransfor, VoltageHourSource voltageHourSource, Map<String, Object> other) {
        voltageHourTransfor.setCarId(voltageHourSource.getCarId());
        voltageHourTransfor.setTime(voltageHourSource.getTime());
        if(voltageHourTransfor.getMinVoltage()==0f){
            voltageHourTransfor.setMinVoltage(voltageHourSource.getVoltage());
        }else
            voltageHourTransfor.setMinVoltage(Math.min(voltageHourSource.getVoltage(), voltageHourTransfor.getMinVoltage()));
        if(voltageHourTransfor.getMaxVoltage()==0f){
            voltageHourTransfor.setMaxVoltage(voltageHourSource.getVoltage());
        }else
            voltageHourTransfor.setMaxVoltage(Math.max(voltageHourTransfor.getMaxVoltage(),voltageHourSource.getVoltage()));
    }


    @Override
    public ExceptionCodeStatus commpareExceptionWithEachData(VoltageHourSource voltageHourSource, VoltageDayLatestData latestData, Map map) {
        if (voltageHourSource.getTime() < latestData.getTime()) {
            log.warn("upload voltage time exception,original data {},latest data :{}",voltageHourSource,latestData);
            return ExceptionCodeStatus.CALC_NO_TIME;
        }

        return ExceptionCodeStatus.CALC_DATA;
    }

    @Override
    public VoltageDayLatestData calcLatestData(VoltageDayLatestData latestData, VoltageHourSource voltageHourSource, Map map, ExceptionCodeStatus status) {
        if(status!=ExceptionCodeStatus.CALC_NO_TIME)
            latestData.setTime(voltageHourSource.getTime());
        latestData.setCarId(voltageHourSource.getCarId());
        if (voltageHourSource.getVoltage() != null && voltageHourSource.getVoltage() > 0) {
            if (latestData.getMaxVoltage() == null || latestData.getMaxVoltage() == 0) {
                latestData.setMaxVoltage(voltageHourSource.getVoltage());
            }else{
                latestData.setMaxVoltage(Math.max(latestData.getMaxVoltage(), voltageHourSource.getVoltage()));
            }
            if(latestData.getMinVoltage() == null || latestData.getMinVoltage() == 0) {
                latestData.setMinVoltage(voltageHourSource.getVoltage());
            }else {
                latestData.setMinVoltage(Math.min(latestData.getMinVoltage(), voltageHourSource.getVoltage()));
            }

        }
        return latestData;
    }

    @Override
    public VoltageDayLatestData initLatestData(VoltageHourSource voltageHourSource, Map map, VoltageDayLatestData latestData) {
        return VoltageDayLatestData.builder().carId(voltageHourSource.getCarId())
                .time(voltageHourSource.getTime())
                .maxVoltage(voltageHourSource.getVoltage()==null?0F:voltageHourSource.getVoltage())
                .minVoltage(voltageHourSource.getVoltage()==null?0F:voltageHourSource.getVoltage())
                .build();
    }

    @Override
    public VoltageDayTransfor calcTransforData(VoltageHourSource latestFirstData, VoltageDayLatestData latestData, VoltageHourSource voltageHourSource, Map map) {
        return VoltageDayTransfor.builder().carId(latestFirstData.getCarId())
                .time(latestFirstData.getTime())
                .maxVoltage(latestData.getMaxVoltage()==null?0F:latestData.getMaxVoltage())
                .minVoltage(latestData.getMinVoltage()==null?0F:latestData.getMinVoltage())
                .build();
    }

    @Override
    public VoltageDayTransfor initFromTempTransfer(VoltageDayLatestData latestData,VoltageHourSource voltageHourSource,Map map,long supplyTime) {
        return VoltageDayTransfor.builder().carId(latestData.getCarId())
                .time(supplyTime)
                .minVoltage(0f)
                .maxVoltage(0f)
                .build();
    }

    @Override
    public Map<String, String> convertData2Map(VoltageDayTransfor transfor,VoltageDayLatestData latestData) {
        return null;
    }

    @Override
    public VoltageDayTransfor calcIntegrityData(VoltageHourSource latestFirstData, VoltageDayLatestData latestData, VoltageDayTransfor voltageDayTransfor,Map map) {
        return VoltageDayTransfor.builder().minVoltage(latestData.getMinVoltage())
                .maxVoltage(latestData.getMaxVoltage())
                .carId(latestData.getCarId())
                .time(latestData.getTime())
                .build();
    }

    private VoltageDayTransfor initTransfor(){
        return VoltageDayTransfor.builder().maxVoltage(0f).minVoltage(0f).build();

    }

    @Override
    public VoltageDayTransfor initFromDormancy(VoltageDayLatestData latestData,long supplyTime) {
        return initFromTempTransfer(latestData,null,null,supplyTime);
    }
}

package cst.jstorm.hour.calcalations.voltage;

import com.cst.stream.stathour.CSTData;
import com.cst.stream.stathour.dormancy.DormancySource;
import com.cst.stream.stathour.voltage.VoltageHourLatestData;
import com.cst.stream.stathour.voltage.VoltageHourSource;
import com.cst.stream.stathour.voltage.VoltageHourTransfor;
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
 * @Description trace小时计算
 * @title
 */
@Slf4j
public class VoltageHourCalcBiz implements DataTransforInterface<VoltageHourTransfor,VoltageHourSource>,
        DataDealTransforInterface<VoltageHourTransfor,VoltageHourSource,VoltageHourLatestData> {
    private final static Logger logger = LoggerFactory.getLogger(VoltageHourCalcBiz.class);
    @Override
    public void execute(VoltageHourTransfor voltageHourTransfor, VoltageHourSource voltageHourSource, Map<String, Object> other) {
        calcData(voltageHourTransfor, voltageHourSource, other);
    }

    @Override
    public VoltageHourTransfor init(VoltageHourSource voltageHourSource, Map<String, Object> other) {
        return VoltageHourTransfor.builder().carId(voltageHourSource.getCarId())
                .time(voltageHourSource.getTime())
                .maxVoltage(Math.max(0f,voltageHourSource.getVoltage()))
                .minVoltage(Math.min(0f,voltageHourSource.getVoltage()))
                .build();
    }

    @Override
    public VoltageHourTransfor initOffet(VoltageHourTransfor voltageHourTransfor, VoltageHourSource voltageHourSource, Map<String, Object> other) {
        return VoltageHourTransfor.builder().carId(voltageHourSource.getCarId())
                .time(voltageHourSource.getTime())
                .maxVoltage(Math.max(voltageHourTransfor.getMaxVoltage(),voltageHourSource.getVoltage()))
                .minVoltage(Math.min(voltageHourTransfor.getMinVoltage(),voltageHourSource.getVoltage()))
                .build();
    }

    @Override
    public VoltageHourTransfor initFromTransfer(VoltageHourTransfor voltageHourTransfor, Map<String, Object> other) {
        return VoltageHourTransfor.builder()
                .time(voltageHourTransfor.getTime())
                .carId(voltageHourTransfor.getCarId())
                .minVoltage(0f)
                .maxVoltage(0f)
                .build();
    }

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
    public ExceptionCodeStatus commpareExceptionWithEachData(VoltageHourSource voltageHourSource, VoltageHourLatestData latestData, Map map) {
        if (voltageHourSource.getTime() < latestData.getTime()) {
            log.warn("upload voltage time exception,original data {},latest data :{}",voltageHourSource,latestData);
            return ExceptionCodeStatus.CALC_NO_TIME;
        }
        if (invalidTooLessThanTime(voltageHourSource.getTime())) {
            log.warn("upload trace time too less than now:original data {},latest data {}", voltageHourSource, latestData);
            return ExceptionCodeStatus.CALC_RETURN;
        }
        return ExceptionCodeStatus.CALC_DATA;
    }

    @Override
    public VoltageHourLatestData calcLatestData(VoltageHourLatestData latestData, VoltageHourSource voltageHourSource, Map map, ExceptionCodeStatus status) {
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
    public VoltageHourLatestData initLatestData(VoltageHourSource voltageHourSource, Map map,VoltageHourLatestData latestData) {
        return VoltageHourLatestData.builder().carId(voltageHourSource.getCarId())
                .time(voltageHourSource.getTime())
                .maxVoltage(voltageHourSource.getVoltage()==null?0F:voltageHourSource.getVoltage())
                .minVoltage(voltageHourSource.getVoltage()==null?0F:voltageHourSource.getVoltage())
                .build();
    }

    @Override
    public VoltageHourTransfor calcTransforData(VoltageHourSource latestFirstData, VoltageHourLatestData latestData, VoltageHourSource voltageHourSource, Map map) {
        return VoltageHourTransfor.builder().carId(latestFirstData.getCarId())
                .time(latestFirstData.getTime())
                .maxVoltage(latestData.getMaxVoltage()==null?0F:latestData.getMaxVoltage())
                .minVoltage(latestData.getMinVoltage()==null?0F:latestData.getMinVoltage())
                .build();
    }

    @Override
    public VoltageHourTransfor initFromTempTransfer(VoltageHourLatestData latestData,VoltageHourSource voltageHourSource,Map map,long supplyTime) {
        return VoltageHourTransfor.builder().carId(latestData.getCarId())
                .time(supplyTime)
                .minVoltage(0f)
                .maxVoltage(0f)
                .build();
    }

    @Override
    public Map<String, String> convertData2Map(VoltageHourTransfor transfor,VoltageHourLatestData latestData) {
        return null;
    }

    @Override
    public VoltageHourTransfor calcIntegrityData(VoltageHourSource latestFirstData, VoltageHourLatestData latestData,VoltageHourTransfor voltageHourTransfor,Map map) {
        return null;
    }

    @Override
    public VoltageHourTransfor initFromDormancy(VoltageHourLatestData latestData, long l1) {
        return null;
    }
}

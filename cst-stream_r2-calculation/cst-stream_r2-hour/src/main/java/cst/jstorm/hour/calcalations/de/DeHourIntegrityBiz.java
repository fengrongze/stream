package cst.jstorm.hour.calcalations.de;

import com.cst.stream.common.JsonHelper;
import com.cst.stream.stathour.de.DeHourTransfor;
import com.fasterxml.jackson.core.type.TypeReference;
import com.cst.jstorm.commons.stream.operations.IntegrityDealInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * de小时完整性计算
 */
public class DeHourIntegrityBiz implements IntegrityDealInterface<DeHourTransfor> {
    private final static Logger logger = LoggerFactory.getLogger(DeHourIntegrityBiz.class);
    @Override
    public Map<String, String> convertData2Map(DeHourTransfor deHourTransfor) {
        Map<String,String> map = null;
            /*if(null!=deHourTransfor){
                map = new HashMap<>();
                map.put("carId",deHourTransfor.getCarId());
                map.put("rapidAccelerationCount",String.valueOf(deHourTransfor.getRapidAccelerationCount()));
                map.put("rapidDecelerationCount",String.valueOf(deHourTransfor.getRapidDecelerationCount()));
                map.put("sharpTurnCount",String.valueOf(deHourTransfor.getSharpTurnCount()));
            }*/
        return map;
    }

    @Override
    public DeHourTransfor initMsg2Data(String msg) {
        DeHourTransfor deHourTransfor = null;
        try {
            deHourTransfor =  JsonHelper.toBean(msg, new TypeReference<DeHourTransfor>() {
            });
        } catch (Exception e) {
            logger.warn("msg convert map error",e);
        }
        return deHourTransfor;
    }
}

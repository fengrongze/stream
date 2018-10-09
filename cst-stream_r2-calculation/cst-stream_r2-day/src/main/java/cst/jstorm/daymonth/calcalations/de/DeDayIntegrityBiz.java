package cst.jstorm.daymonth.calcalations.de;

import com.cst.stream.common.JsonHelper;
import com.cst.stream.stathour.de.DeDayTransfor;
import com.fasterxml.jackson.core.type.TypeReference;
import com.cst.jstorm.commons.stream.operations.IntegrityDealInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * de完整性计算
 */
public class DeDayIntegrityBiz implements IntegrityDealInterface<DeDayTransfor> {
    private final static Logger logger = LoggerFactory.getLogger(DeDayIntegrityBiz.class);
    @Override
    public Map<String, String> convertData2Map(DeDayTransfor deDayTransfor) {
        Map<String,String> map = null;
          /*  if(null!=deDayTransfor){
                map = new HashMap<>();
                map.put("carId",deDayTransfor.getCarId());
                map.put("rapidAccelerationCount",String.valueOf(deDayTransfor.getRapidAccelerationCount()));
                map.put("rapidDecelerationCount",String.valueOf(deDayTransfor.getRapidDecelerationCount()));
                map.put("sharpTurnCount",String.valueOf(deDayTransfor.getSharpTurnCount()));
            }*/
        return map;
    }

    @Override
    public DeDayTransfor initMsg2Data(String msg) {
        DeDayTransfor deDayTransfor = null;
        try {
            deDayTransfor =  JsonHelper.toBeanWithoutException(msg, new TypeReference<DeDayTransfor>() {
            });
        } catch (Exception e) {
            logger.warn("msg convert map error",e);
        }
        return deDayTransfor;
    }
}

package cst.jstorm.daymonth.calcalations.am;

import com.cst.stream.common.JsonHelper;
import com.cst.stream.stathour.am.AmDayTransfor;
import com.fasterxml.jackson.core.type.TypeReference;
import com.cst.jstorm.commons.stream.operations.IntegrityDealInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * am完整性计算
 */
public class AmDayIntegrityBiz implements IntegrityDealInterface<AmDayTransfor> {
    private final static Logger logger = LoggerFactory.getLogger(AmDayIntegrityBiz.class);
    @Override
    public Map<String, String> convertData2Map(AmDayTransfor amDayTransfor) {
        Map<String,String> map = null;
           /* if(null!=amDayTransfor){
                map = new HashMap<>();
                map.put("carId",amDayTransfor.getCarId());
                map.put("ignition",String.valueOf(amDayTransfor.getIgnition()));
                map.put("flameOut",String.valueOf(amDayTransfor.getFlameOut()));
                map.put("insertNum",String.valueOf(amDayTransfor.getInsertNum()));
                map.put("collision",String.valueOf(amDayTransfor.getCollision()));
                map.put("overSpeed",String.valueOf(amDayTransfor.getOverSpeed()));
                map.put("isMissing",String.valueOf(amDayTransfor.getIsMissing()));
                map.put("pulloutTimes",String.valueOf(amDayTransfor.getPulloutTimes()));
                map.put("isFatigue",String.valueOf(amDayTransfor.getIsFatigue()));
            }*/

        return map;
    }

    @Override
    public AmDayTransfor initMsg2Data(String msg) {
        AmDayTransfor amDayTransfor = null;
        try {
             amDayTransfor =  JsonHelper.toBeanWithoutException(msg, new TypeReference<AmDayTransfor>() {
            });
        } catch (Exception e) {
            logger.warn("msg convert map error",e);
        }
        return amDayTransfor;
    }
}

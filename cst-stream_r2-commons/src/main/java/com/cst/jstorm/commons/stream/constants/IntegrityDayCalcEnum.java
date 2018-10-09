package com.cst.jstorm.commons.stream.constants;

import java.util.HashMap;
import java.util.Map;

/**
 * 完整性天计算enum
 */
public enum  IntegrityDayCalcEnum {
    OBD("obdDayData",1),
    DE("deDayData",2),
    AM("amDayData",4),
    GPS("gpsDayData",8),
    STATUS("statusDayData",16),
    TRACE("traceDayData",32),
    TRACE_DELETE("traceDeleteDayData",64),
    VOLTAGE("voltageDayData",128);

    private String dayDataType;
    private int value;

    IntegrityDayCalcEnum(String dayDataType,int value) {
        this.dayDataType = dayDataType;
        this.value = value;
    }

    public String getDayDataType() {
        return dayDataType;
    }

    public void setDayDataType(String dayDataType) {
        this.dayDataType = dayDataType;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    private static Map<Integer,IntegrityDayCalcEnum> integrityDayCalcEnumMap;
    public static Map<Integer,IntegrityDayCalcEnum> toMap(){
        if(integrityDayCalcEnumMap==null){
            integrityDayCalcEnumMap=new HashMap<>();
            for(IntegrityDayCalcEnum integrityDayCalcEnum:IntegrityDayCalcEnum.values()){
                integrityDayCalcEnumMap.put(integrityDayCalcEnum.getValue(), integrityDayCalcEnum);
            }
        }
        return integrityDayCalcEnumMap;
    }
}

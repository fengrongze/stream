package com.cst.jstorm.commons.stream.constants;

import java.util.HashMap;
import java.util.Map;

/**
 * 完整性小时计算enum
 */
public enum IntegrityHourCalcEnum {
    OBD("obdHourData",1),
    DE("deHourData",2),
    AM("amHourData",4),
    GPS("gpsHourData",8),
    STATUS("statusHourData",16),
    TRACE("traceHourData",32),
    TRACE_DELETE("traceDeleteHourData",64),
    VOLTAGE("voltageHourData",128);

    private String hourDataType;
    private int value;

    IntegrityHourCalcEnum(String hourDataType, int value) {
        this.hourDataType = hourDataType;
        this.value = value;
    }

    public String getHourDataType() {
        return hourDataType;
    }

    public void setHourDataType(String hourDataType) {
        this.hourDataType = hourDataType;
    }

    public static Map<Integer, IntegrityHourCalcEnum> getIntegrityHourCalcEnumMap() {
        return integrityHourCalcEnumMap;
    }

    public static void setIntegrityHourCalcEnumMap(Map<Integer, IntegrityHourCalcEnum> integrityHourCalcEnumMap) {
        IntegrityHourCalcEnum.integrityHourCalcEnumMap = integrityHourCalcEnumMap;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    private static Map<Integer,IntegrityHourCalcEnum> integrityHourCalcEnumMap;
    public static Map<Integer,IntegrityHourCalcEnum> toMap(){
        if(integrityHourCalcEnumMap ==null){
            integrityHourCalcEnumMap =new HashMap<>();
            for(IntegrityHourCalcEnum integrityHourCalcEnum:IntegrityHourCalcEnum.values()){
                integrityHourCalcEnumMap.put(integrityHourCalcEnum.getValue(), integrityHourCalcEnum);
            }
        }
        return integrityHourCalcEnumMap;
    }
}

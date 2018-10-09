package com.cst.stream.common;

/**
 * @author Johnney.chiu
 * create on 2018/1/24 16:43
 * @Description hbase的columns配置
 */
public class HbaseColumn {

    public static final class HourSourceColumn{
        //am hour data
        public static final String[] amHourColumns = new String[]{"carId", "time" , "alarmType","troubleCode","gatherType","longitude","latitude","versionType"};

        //de hour data
        public static final String[] deHourColumns = new String[]{"carId", "time",  "speed",  "actType",  "gatherType",  "rapidAccelerationCounts",  "rapidDecelerationCounts",  "sharpTurnCounts"};

        //gps hour data
        public static final String[] gpsHourColumns = new String[]{"carId", "time"
                ,    "satellites",  "longitude",  "latitude"};


        //obd hour data
        public static final String[] obdHourColumns = new String[]{"carId", "time",
                "din","speed",  "totalDistance",  "totalFuel",  "runTotalTime",
                "motormeterDistance","powerNum"};

        //trace hour data
        public static final String[] traceHourColumns = new String[]{"carId", "time"
                , "travelUuid",  "startTime",  "stopTime",  "tripDistance",  "travelType",  "tripDriveTime"};

        //trace delete hour data
        public static final String[] traceDeleteHourColumns = new String[]{"carId", "time"
                ,"dinData"};

        //voltage hour data
        public static final String[] voltageHourColumns = new String[]{"carId", "time"
                ,"voltage"};

        //mileage hour data
        public static final String[] mileageHourColumns = new String[]{"carId", "time"
                ,"gpsSpeed","obdSpeed","milGpsTotalDistance","milObdTotalDistance","milTotalFuel","milRunTotalTime","panelDistance"};

    }

    public static final class DaySourceColumn{
        //am day data
        public static final String[] amDayColumns = new String[]{"carId", "time","alarmType","troubleCode","gatherType","longitude","latitude","versionType"};

        //de day data
        public static final String[] deDayColumns = new String[]{"carId", "time",  "speed",  "actType",  "gatherType",  "rapidAccelerationCounts",  "rapidDecelerationCounts",  "sharpTurnCounts"};

        //gps day data
        public static final String[] gpsDayColumns = new String[]{"carId", "time"
                ,    "satellites",  "longitude",  "latitude"};


        //obd day data
        public static final String[] obdDayColumns = new String[]{"carId", "time",
                "din",  "speed",  "totalDistance",  "totalFuel",  "runTotalTime",
                "motormeterDistance","powerNum"};

        //trace day data
        public static final String[] traceDayColumns = new String[]{"carId", "time"
                , "travelUuid",  "startTime",  "stopTime",  "tripDistance",  "travelType",  "tripDriveTime"};

        //trace delete day data
        public static final String[] traceDeleteDayColumns = new String[]{"carId", "time"
                ,"dinData"};

        //voltage day data
        public static final String[] voltageDayColumns = new String[]{"carId", "time"
                ,"voltage"};

        //mileage day data
        public static final String[] mileageDayColumns = new String[]{"carId", "time"
                ,"gpsSpeed","obdSpeed","milGpsTotalDistance","milObdTotalDistance","milTotalFuel","milRunTotalTime","panelDistance"};


    }


    public static final class HourStatisticsCloumn {
        //am hour data
        public static final String[] amHourColumns = new String[]{"carId", "time", "ignition", "flameOut", "insertNum", "collision"
                , "overSpeed", "isMissing", "pulloutTimes", "isFatigue","pulloutCounts"};

        //de hour data
        public static final String[] deHourColumns = new String[]{"carId", "time",
                "rapidAccelerationCount", "rapidDecelerationCount", "sharpTurnCount"};

        //gps hour data
        public static final String[] gpsHourColumns = new String[]{"carId", "time"
                , "maxSatelliteNum", "gpsCount", "isNonLocal"};


        //obd hour data
        public static final String[] obdHourColumns = new String[]{"carId", "time"
                , "din", "speed", "totalDistance", "totalFuel", "runTotalTime", "motormeterDistance", "maxSpeed",
                "isHighSpeed",  "isDrive", "mileage", "fuel", "duration",
                "fee","fuelPerHundred" ,"din", "dinChange","toolingProbability","averageSpeed","powerConsumption"};

        //trace hour data
        public static final String[] traceHourColumns = new String[]{"carId", "time"
                ,"traceCounts"};

        //trace delete hour data
        public static final String[] traceDeleteHourColumns = new String[]{"carId", "time"
                ,"traceDeleteCounts"};

        //voltage hour data
        public static final String[] voltageHourColumns = new String[]{"carId", "time"
                ,"maxVoltage","minVoltage"};

        //mileage hour data
        public static final String[] mileageHourColumns = new String[]{"carId", "time"
                ,"milGpsMileage","milObdMileage","milPanelMileage","milFuel","milDuration","milFee","milObdMaxSpeed","milGpsMaxSpeed","gpsSpeed","obdSpeed","milGpsTotalDistance","milObdTotalDistance","milTotalFuel","milRunTotalTime","panelDistance"};
        public static final String[] allColumns = new String[]{"carId", "time", "ignition", "flameOut", "insertNum", "collision"
                , "overSpeed", "isMissing", "pulloutTimes", "isFatigue", "pulloutCounts",
                "rapidAccelerationCount", "rapidDecelerationCount", "sharpTurnCount",
                "maxSatelliteNum", "gpsCount", "din", "dinChange", "isNonLocal",
                "speed", "totalDistance", "totalFuel", "runTotalTime", "motormeterDistance", "maxSpeed",
                "isHighSpeed", "isDrive", "mileage", "fuel", "duration", "fee",
                "traceCounts", "traceDeleteCounts", "maxVoltage", "minVoltage",
                "fuelPerHundred", "toolingProbability", "averageSpeed", "pulloutCounts","powerConsumption"
                ,"milGpsMileage","milObdMileage","milPanelMileage","milFuel","milDuration","milFee","milObdMaxSpeed","milGpsMaxSpeed","gpsSpeed","obdSpeed","milGpsTotalDistance","milObdTotalDistance","milTotalFuel","milRunTotalTime","panelDistance"
        };

    }



    public static final class HourNoDelayStatisticsCloumn {
        //am hour data
        public static final String[] amHourColumns = new String[]{"carId", "time", "ignition", "flameOut", "insertNum", "collision"
                , "overSpeed", "isMissing", "pulloutTimes", "isFatigue"};

        //de hour data
        public static final String[] deHourColumns = new String[]{"carId", "time",
                "rapidAccelerationCount", "rapidDecelerationCount", "sharpTurnCount"};

        //gps hour data
        public static final String[] gpsHourColumns = new String[]{"carId", "time"
                , "maxSatelliteNum", "gpsCount","isNonLocal"};


        //obd hour data
        public static final String[] obdHourColumns = new String[]{"carId", "time"
                , "din", "speed", "totalDistance", "totalFuel", "runTotalTime", "motormeterDistance", "maxSpeed",
                "isHighSpeed", "isNightDrive", "isDrive", "mileage", "fuel", "duration",
                "fee", "din", "dinChange"};

        // trace hour data
        public static final String[] traceHourColumns = new String[]{"carId", "time"
                ,"traceCounts"};

        //trace delete hour data
        public static final String[] traceDeleteHourColumns = new String[]{"carId", "time"
                ,"traceDeleteCounts"};

        //voltage hour data
        public static final String[] voltageHourColumns = new String[]{"carId", "time"
                ,"maxVoltage","minVoltage"};

    }

    public static final class DayStatisticsCloumn {
        //am day data
        public static final String[] amDayColumns = new String[]{"carId", "time"
                , "ignition", "flameOut", "insertNum", "collision", "overSpeed"
                , "isMissing", "pulloutTimes", "isFatigue","pulloutCounts"};

        //de day data
        public static final String[] deDayColumns = new String[]{"carId", "time",
                "rapidAccelerationCount", "rapidDecelerationCount", "sharpTurnCount"};

        //gps day data
        public static final String[] gpsDayColumns = new String[]{"carId", "time"
                , "maxSatelliteNum", "gpsCount", "isNonLocal"};


        //obd day data
        public static final String[] obdDayColumns = new String[]{"carId", "time"
                ,  "totalDistance", "totalFuel", "maxSpeed",
                "isHighSpeed", "isNightDrive", "isDrive", "mileage", "fuel", "duration",
                "fee","runTotalTime", "motormeterDistance","speed",
                "din", "dinChange","fuelPerHundred","toolingProbability","averageSpeed","powerConsumption"};

        // trace day data
        public static final String[] traceDayColumns = new String[]{"carId", "time"
                ,"traceCounts"};

        //trace delete day data
        public static final String[] traceDeleteDayColumns = new String[]{"carId", "time"
                ,"traceDeleteCounts"};

        //voltage day data
        public static final String[] voltageDayColumns = new String[]{"carId", "time"
                ,"maxVoltage","minVoltage"};
        //mileage day data
        public static final String[] mileageDayColumns = new String[]{"carId", "time"
                ,"milGpsMileage","milObdMileage","milPanelMileage","milFuel","milDuration","milFee","milObdMaxSpeed","milGpsMaxSpeed","gpsSpeed","obdSpeed","milGpsTotalDistance","milObdTotalDistance","milTotalFuel","milRunTotalTime","panelDistance"};

        public static final String[] allColumns = new String[]{
                "carId", "time", "ignition", "flameOut", "insertNum", "collision", "overSpeed", "isMissing", "pulloutTimes","pulloutCounts", "isFatigue",
                "rapidAccelerationCount", "rapidDecelerationCount", "sharpTurnCount",
                "maxSatelliteNum", "gpsCount", "din", "dinChange", "isNonLocal",
                "totalDistance", "totalFuel", "maxSpeed",
                "isHighSpeed", "isNightDrive", "isDrive", "mileage", "fuel", "duration", "fee","runTotalTime", "motormeterDistance","speed",
                "traceCounts","traceDeleteCounts","maxVoltage","minVoltage","fuelPerHundred",
                "toolingProbability","averageSpeed", "pulloutCounts","powerConsumption"
                ,"milGpsMileage","milObdMileage","milPanelMileage","milFuel","milDuration","milFee","milObdMaxSpeed","milGpsMaxSpeed","gpsSpeed","obdSpeed","milGpsTotalDistance","milObdTotalDistance","milTotalFuel","milRunTotalTime","panelDistance"
        };
    }

    public static final class MonthStatisticsCloumn {
        //am month data
        public static final String[] amMonthColumns = new String[]{"carId", "time"
                , "ignition", "flameOut", "insertNum", "collision", "overSpeed"
                , "missingNum", "pulloutTimes", "fatigueNum","pulloutCounts"};

        //de month data
        public static final String[] deMonthColumns = new String[]{"carId", "time",
                "rapidAccelerationCount", "rapidDecelerationCount", "sharpTurnCount"};

        //gps month data
        public static final String[] gpsMonthColumns = new String[]{"carId", "time"
                , "maxSatelliteNum", "gpsCount", "nonLocalNum"};


        //obd month data
        public static final String[] obdMonthColumns = new String[]{"carId", "time"
                , "totalDistance", "totalFuel", "maxSpeed",
                "highSpeedNum", "nightDriveNum", "driveNum", "mileage", "fuel", "duration", "fee"
                ,"runTotalTime", "motormeterDistance","speed","fuelPerHundred", "din", "dinChangeNum"
                ,"toolingProbability","averageSpeed","powerConsumption"};

        // trace month data
        public static final String[] traceMonthColumns = new String[]{"carId", "time"
                ,"traceCounts"};

        //trace delete month data
        public static final String[] traceDeleteMonthColumns = new String[]{"carId", "time"
                ,"traceDeleteCounts"};

        //voltage month data
        public static final String[] voltageMonthColumns = new String[]{"carId", "time"
                ,"maxVoltage","minVoltage"};
        //mileage month data
        public static final String[] mileageMonthColumns = new String[]{"carId", "time"
                ,"milGpsMileage","milObdMileage","milPanelMileage","milFuel","milDuration","milFee","milObdMaxSpeed","milGpsMaxSpeed","gpsSpeed","obdSpeed","milGpsTotalDistance","milObdTotalDistance","milTotalFuel","milRunTotalTime","panelDistance"};

        public static final String[] allColumns = new String[]{
                "carId", "time", "ignition", "flameOut", "insertNum", "collision",
                "overSpeed", "missingNum", "pulloutTimes", "fatigueNum","pulloutCounts",
                "rapidAccelerationCount", "rapidDecelerationCount", "sharpTurnCount",
                "maxSatelliteNum", "gpsCount", "din", "dinChangeNum", "nonLocalNum",
                "totalDistance", "totalFuel", "maxSpeed",
                "highSpeedNum", "nightDriveNum", "driveNum", "mileage", "fuel", "duration", "fee","runTotalTime", "motormeterDistance","speed",
                "traceCounts","traceDeleteCounts","maxVoltage","minVoltage","fuelPerHundred",
                "toolingProbability","averageSpeed", "pulloutCounts","powerConsumption"
                ,"milGpsMileage","milObdMileage","milPanelMileage","milFuel","milDuration","milFee","milObdMaxSpeed","milGpsMaxSpeed","gpsSpeed","obdSpeed","milGpsTotalDistance","milObdTotalDistance","milTotalFuel","milRunTotalTime","panelDistance"
        };
    }

    public static final class YearStatisticsCloumn {
        //am year data
        public static final String[] amYearColumns = new String[]{"carId", "time"
                , "ignition", "flameOut", "insertNum", "collision", "overSpeed"
                , "missingNum", "pulloutTimes", "fatigueNum","pulloutCounts"};

        //de year data
        public static final String[] deYearColumns = new String[]{"carId", "time",
                "rapidAccelerationCount", "rapidDecelerationCount", "sharpTurnCount"};

        //gps year data
        public static final String[] gpsYearColumns = new String[]{"carId", "time"
                , "maxSatelliteNum", "gpsCount", "nonLocalNum"};


        //obd year data
        public static final String[] obdYearColumns = new String[]{"carId", "time"
                , "totalDistance", "totalFuel", "maxSpeed",
                "highSpeedNum", "nightDriveNum", "driveNum", "mileage", "fuel", "duration", "fee"
                ,"runTotalTime", "motormeterDistance","speed","fuelPerHundred", "din", "dinChangeNum"
                ,"toolingProbability","averageSpeed","powerConsumption"};

        // trace year data
        public static final String[] traceYearColumns = new String[]{"carId", "time"
                ,"traceCounts"};

        //trace delete year data
        public static final String[] traceDeleteYearColumns = new String[]{"carId", "time"
                ,"traceDeleteCounts"};

        //voltage year data
        public static final String[] voltageYearColumns = new String[]{"carId", "time"
                ,"maxVoltage","minVoltage"};
        //mileage year data
        public static final String[] mileageYearColumns = new String[]{"carId", "time"
                ,"milGpsMileage","milObdMileage","milPanelMileage","milFuel","milDuration","milFee","milObdMaxSpeed","milGpsMaxSpeed","gpsSpeed","obdSpeed","milGpsTotalDistance","milObdTotalDistance","milTotalFuel","milRunTotalTime","panelDistance"};

        public static final String[] allColumns = new String[]{
                "carId", "time", "ignition", "flameOut", "insertNum", "collision",
                "overSpeed", "missingNum", "pulloutTimes", "fatigueNum","pulloutCounts",
                "rapidAccelerationCount", "rapidDecelerationCount", "sharpTurnCount",
                "maxSatelliteNum", "gpsCount", "din", "dinChangeNum", "nonLocalNum",
                "totalDistance", "totalFuel", "maxSpeed",
                "highSpeedNum", "nightDriveNum", "driveNum", "mileage", "fuel", "duration", "fee","runTotalTime", "motormeterDistance","speed",
                "traceCounts","traceDeleteCounts","maxVoltage","minVoltage","fuelPerHundred",
                "toolingProbability","averageSpeed", "pulloutCounts","powerConsumption"
                ,"milGpsMileage","milObdMileage","milPanelMileage","milFuel","milDuration","milFee","milObdMaxSpeed","milGpsMaxSpeed","gpsSpeed","obdSpeed","milGpsTotalDistance","milObdTotalDistance","milTotalFuel","milRunTotalTime","panelDistance"
        };
    }

    public static final class DayNoDelayStatisticsCloumn {
        //am day data
        public static final String[] amDayColumns = new String[]{"carId", "time"
                , "ignition", "flameOut", "insertNum", "collision", "overSpeed", "isMissing", "pulloutTimes", "isFatigue"};

        //de day data
        public static final String[] deDayColumns = new String[]{"carId", "time",
                "rapidAccelerationCount", "rapidDecelerationCount", "sharpTurnCount"};

        //gps day data
        public static final String[] gpsDayColumns = new String[]{"carId", "time"
                , "maxSatelliteNum", "gpsCount", "din", "dinChange", "isNonLocal"};


        //obd day data
        public static final String[] obdDayColumns = new String[]{"carId", "time"
                , "din", "totalDistance", "totalFuel", "maxSpeed",
                "isHighSpeed", "isNightDrive", "isDrive", "mileage", "fuel", "duration", "fee",
                "runTotalTime", "motormeterDistance","speed","fuelPerHundred"};


        // trace day data
        public static final String[] traceDayColumns = new String[]{"carId", "time"
                ,"traceCounts"};

        //trace delete day data
        public static final String[] traceDeleteDayColumns = new String[]{"carId", "time"
                ,"traceDeleteCounts"};

        //voltage day data
        public static final String[] voltageDayColumns = new String[]{"carId", "time"
                ,"maxVoltage","minVoltage"};

    }

}

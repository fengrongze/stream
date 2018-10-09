package com.cst.jstorm.commons.stream.constants;

/**
 * @author Johnney.chiu
 * create on 2017/11/17 16:57
 * @Description 定义所有的Spout、Bolt的名称
 */
public enum ComponentNameEnum {
    GPS("gps_spout", "gps_hour_bolt","gps_hour_persist_bolt","gps_day_bolt","gps_day_persist_bolt","gps_hour_no_delay_persist_bolt","gps_day_no_delay_persist_bolt","gps_spout_day","gps_hour_first_data_bolt","gps_day_first_data_bolt", "gps_month_bolt","gps_month_persist_bolt", "gps_year_bolt","gps_year_persist_bolt","",""),
    OBD("obd_spout", "obd_hour_bolt","obd_hour_persist_bolt","obd_day_bolt","obd_day_persist_bolt","obd_hour_no_delay_persist_bolt","obd_day_no_delay_persist_bolt","obd_spout_day","obd_hour_first_data_bolt","obd_day_first_data_bolt", "obd_month_bolt","obd_month_persist_bolt", "obd_year_bolt","obd_year_persist_bolt","",""),
    MILEAGE("mileage_spout", "mileage_hour_bolt","mileage_hour_persist_bolt","mileage_day_bolt","mileage_day_persist_bolt","mileage_hour_no_delay_persist_bolt","mileage_day_no_delay_persist_bolt","mileage_spout_day","mileage_hour_first_data_bolt","mileage_day_first_data_bolt", "mileage_month_bolt","mileage_month_persist_bolt", "mileage_year_bolt","mileage_year_persist_bolt","",""),
    ELECTRIC_OBD("electric_obd_spout", "electric_obd_hour_bolt","electric_obd_hour_persist_bolt","electric_obd_day_bolt","electric_obd_day_persist_bolt","electric_obd_hour_no_delay_persist_bolt","electric_obd_day_no_delay_persist_bolt","electric_obd_spout_day","electric_obd_hour_first_data_bolt","electric_obd_day_first_data_bolt", "electric_obd_month_bolt","electric_obd_month_persist_bolt", "electric_obd_year_bolt","electric_obd_year_persist_bolt","",""),
    AM("am_spout", "am_hour_bolt","am_hour_persist_bolt","am_day_bolt","am_day_persist_bolt","am_hour_no_delay_persist_bolt","am_day_no_delay_persist_bolt","am_spout_day","am_hour_first_data_bolt","am_day_first_data_bolt", "am_month_bolt","am_month_persist_bolt", "am_year_bolt","am_year_persist_bolt","",""),
    DE("de_spout", "de_hour_bolt","de_hour_persist_bolt","de_day_bolt","de_day_persist_bolt","de_hour_no_delay_persist_bolt","de_day_no_delay_persist_bolt","de_spout_day","de_hour_first_data_bolt","de_day_first_data_bolt", "de_month_bolt","de_month_persist_bolt", "de_year_bolt","de_year_persist_bolt","",""),
    STATUS("status_spout", "status_hour_bolt","status_hour_persist_bolt","status_day_bolt","status_day_persist_bolt","status_hour_no_delay_persist_bolt","status_day_no_delay_persist_bolt","status_spout_day","status_hour_first_data_bolt","status_day_first_data_bolt", "status_month_bolt","status_month_persist_bolt", "status_year_bolt","status_year_persist_bolt","",""),
    TRACE("trace_spout", "trace_bolt","trace_hour_persist_bolt","trace_day_bolt","trace_day_persist_bolt","trace_hour_no_delay_persist_bolt","trace_day_no_delay_persist_bolt","trace_spout_day","trace_hour_first_data_bolt","trace_day_first_data_bolt", "trace_month_bolt","trace_month_persist_bolt", "trace_year_bolt","trace_year_persist_bolt","",""),
    TRACE_DELETE("trace_delete_spout", "trace_delete_bolt","trace_delete_hour_persist_bolt","trace_delete_day_bolt","trace_delete_day_persist_bolt","trace_delete_hour_no_delay_persist_bolt","trace_delete_day_no_delay_persist_bolt","trace_delete_spout_day","trace_delete_hour_first_data_bolt","trace_delete_day_first_data_bolt", "trace_delete_month_bolt","trace_delete_month_persist_bolt", "trace_delete_year_bolt","trace_delete_year_persist_bolt","",""),
    VOLTAGE("voltage_spout", "voltage_bolt","voltage_hour_persist_bolt","voltage_day_bolt","voltage_day_persist_bolt","voltage_hour_no_delay_persist_bolt","voltage_day_no_delay_persist_bolt","voltage_spout_day","voltage_hour_first_data_bolt","voltage_day_first_data_bolt", "voltage_month_bolt","voltage_month_persist_bolt", "voltage_year_bolt","voltage_year_persist_bolt","",""),
    OTHER("other_spout", "other_bolt","other_hour_persist_bolt","other_day_bolt","other_day_persist_bolt","other_hour_no_delay_persist_bolt","other_day_no_delay_persist_bolt","other_spout_day","other_hour_first_data_bolt","other_day_first_data_bolt", "other_month_bolt","other_month_persist_bolt", "other_year_bolt","other_year_persist_bolt","",""),
    INTEGRATED("NOT_SPOUT","NOT_HOUR_CALC","hour_data_persist_bolt","NOT_DAY_CALC","day_data_persist_bolt","hour_no_delay_persist_bolt","day_no_delay_persist_bolt","not_a_spout_day","not_hour_first_data_bolt","not_day_first_data_bolt", "not_month_bolt","not_month_persist_bolt", "not_year_bolt","not_year_persist_bolt","hour_dispatch_bolt","day_dispatch_bolt"),
    ZONE_SCHEDULED("NOT_SPOUT","NOT_HOUR_CALC","NOT_HOUR_PERSIST","day_data_schedule_calc_bolt","NOT_DAY_PERSIST","NOT_DELAY_CALC","NOT_DELAY_PERSIST","zone_scheduled_spout_day","not_hour_first_data_bolt","not_day_first_data_bolt", "not_month_bolt","not_month_persist_bolt", "not_year_bolt","not_year_persist_bolt","not_hour_dispatch_bolt","not_day_dispatch_bolt"),
    DORMANCY_SCHEDULED("NOT_SPOUT","NOT_HOUR_CALC","NOT_HOUR_PERSIST","day_data_dormancy_calc_bolt","NOT_DAY_PERSIST","NOT_DELAY_CALC","NOT_DELAY_PERSIST","dormancy_spout_day","not_hour_first_data_bolt","not_day_first_data_bolt", "not_month_bolt","not_month_persist_bolt", "not_year_bolt","not_year_persist_bolt","not_hour_dispatch_bolt","not_day_dispatch_bolt");

    private String spoutName;
    private String hourCalcBoltName;
    private String hourPersistBoltName;
    private String dayCalcBoltName;
    private String dayPersistBoltName;
    private String hourNoDelaypersistBoltName;
    private String dayNoDelaypersistBoltName;
    private String spoutNameOther;
    private String firstSourceBoltName;
    private String dayFirstSourceBoltName;
    private String monthCalcBoltName;
    private String monthPersistBoltName;
    private String yearCalcBoltName;
    private String yearPersistBoltName;
    private String hourDispatchBoltName;
    private String dayDispatchBoltName;



    ComponentNameEnum(String spoutName, String hourCalcBoltName, String hourPersistBoltName,
                      String dayCalcBoltName, String dayPersistBoltName,
                      String hourNoDelaypersistBoltName,String dayNoDelaypersistBoltName,String spoutNameOther,
                      String firstSourceBoltName,String dayFirstSourceBoltName,
                      String monthCalcBoltName,String monthPersistBoltName,String yearCalcBoltName,String yearPersistBoltName,String hourDispatchBoltName,String dayDispatchBoltName) {
        this.spoutName = spoutName;
        this.hourCalcBoltName = hourCalcBoltName;
        this.hourPersistBoltName = hourPersistBoltName;
        this.dayCalcBoltName = dayCalcBoltName;
        this.dayPersistBoltName = dayPersistBoltName;
        this.hourNoDelaypersistBoltName = hourNoDelaypersistBoltName;
        this.dayNoDelaypersistBoltName = dayNoDelaypersistBoltName;
        this.spoutNameOther = spoutNameOther;
        this.firstSourceBoltName = firstSourceBoltName;
        this.dayFirstSourceBoltName = dayFirstSourceBoltName;
        this.monthCalcBoltName = monthCalcBoltName;
        this.monthPersistBoltName = monthPersistBoltName;
        this.yearCalcBoltName = yearCalcBoltName;
        this.yearPersistBoltName = yearPersistBoltName;
        this.hourDispatchBoltName = hourDispatchBoltName;
        this.dayDispatchBoltName = dayDispatchBoltName;
    }

    public String getSpoutName() {
        return spoutName;
    }

    public String getHourCalcBoltName() {
        return hourCalcBoltName;
    }

    public String getHourPersistBoltName() {
        return hourPersistBoltName;
    }

    public String getDayCalcBoltName() {
        return dayCalcBoltName;
    }

    public String getDayPersistBoltName() {
        return dayPersistBoltName;
    }

    public String getHourNoDelaypersistBoltName() {
        return hourNoDelaypersistBoltName;
    }

    public String getDayNoDelaypersistBoltName() {
        return dayNoDelaypersistBoltName;
    }

    public String getSpoutNameOther() {
        return spoutNameOther;
    }


    public String getFirstSourceBoltName() {
        return firstSourceBoltName;
    }

    public String getDayFirstSourceBoltName() {
        return dayFirstSourceBoltName;
    }

    public String getMonthCalcBoltName() {
        return monthCalcBoltName;
    }

    public String getMonthPersistBoltName() {
        return monthPersistBoltName;
    }

    public String getYearCalcBoltName() {
        return yearCalcBoltName;
    }

    public String getYearPersistBoltName() {
        return yearPersistBoltName;
    }

    public String getHourDispatchBoltName() {
        return hourDispatchBoltName;
    }

    public String getDayDispatchBoltName() {
        return dayDispatchBoltName;
    }
}

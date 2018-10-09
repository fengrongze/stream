package com.cst.stream.common;

/**
 * @author Johnney.chiu
 * create on 2017/12/19 16:38
 * @Description Hbase的表配置
 */
public enum HBaseTable {



    HOUR_STATISTICS("cst_stream_hour_statistics", "obd","gps","am","de","trace","tracedelete","voltage","mileage"),
    DAY_STATISTICS("cst_stream_day_statistics", "obd","gps","am","de","trace","tracedelete","voltage","mileage"),
    MONTH_STATISTICS("cst_stream_month_statistics", "obd","gps","am","de","trace","tracedelete","voltage","mileage"),
    YEAR_STATISTICS("cst_stream_year_statistics", "obd","gps","am","de","trace","tracedelete","voltage","mileage"),



    HOUR_FIRST_ZONE("cst_stream_hour_first_source","info","","","","","","",""),
    DAY_FIRST_ZONE("cst_stream_day_first_source","info","","","","","","",""),

   ;




    private String tableName;
    private String firstFamilyName;
    private String secondFamilyName;
    private String thirdFamilyName;
    private String fourthFamilyName;
    private String fifthFamilyName;
    private String sixthFamilyName;
    private String seventhFamilyName;
    private String eighthFamilyName;


    private HBaseTable(String tableName, String firstFamilyName, String secondFamilyName, String thirdFamilyName, String fourthFamilyName, String fifthFamilyName,
                       String sixthFamilyName, String seventhFamilyName,String eighthFamilyName) {
        this.tableName = tableName;
        this.firstFamilyName = firstFamilyName;
        this.secondFamilyName = secondFamilyName;
        this.thirdFamilyName = thirdFamilyName;
        this.fourthFamilyName = fourthFamilyName;
        this.fifthFamilyName = fifthFamilyName;
        this.sixthFamilyName = sixthFamilyName;
        this.seventhFamilyName = seventhFamilyName;
        this.eighthFamilyName = eighthFamilyName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getFirstFamilyName() {
        return firstFamilyName;
    }

    public String getSecondFamilyName() {
        return secondFamilyName;
    }

    public String getThirdFamilyName() {
        return thirdFamilyName;
    }

    public String getFourthFamilyName() {
        return fourthFamilyName;
    }

    public String getFifthFamilyName() {
        return fifthFamilyName;
    }

    public String getSixthFamilyName() {
        return sixthFamilyName;
    }

    public String getSeventhFamilyName() {
        return seventhFamilyName;
    }

    public String getEighthFamilyName() {
        return eighthFamilyName;
    }
}

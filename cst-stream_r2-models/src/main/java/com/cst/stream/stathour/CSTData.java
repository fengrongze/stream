package com.cst.stream.stathour;

import com.cst.stream.stathour.am.*;
import com.cst.stream.stathour.de.*;
import com.cst.stream.stathour.dormancy.DormancySource;
import com.cst.stream.stathour.gps.*;
import com.cst.stream.stathour.integrated.DayIntegratedTransfor;
import com.cst.stream.stathour.integrated.HourIntegratedTransfor;
import com.cst.stream.stathour.integrated.MonthIntegratedTransfor;
import com.cst.stream.stathour.integrated.YearIntegratedTransfor;
import com.cst.stream.stathour.mileage.*;
import com.cst.stream.stathour.obd.*;
import com.cst.stream.stathour.trace.*;
import com.cst.stream.stathour.tracedelete.*;
import com.cst.stream.stathour.voltage.*;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

/**
 * @author Johnney.chiu
 * create on 2017/12/1 11:09
 * @Description 数据源的父类
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
        @JsonSubTypes.Type(value = GpsHourSource.class, name = "GHS"),
        @JsonSubTypes.Type(value = GpsHourLatestData.class, name = "GHL"),
        @JsonSubTypes.Type(value = GpsDayTransfor.class, name = "GDT"),
        @JsonSubTypes.Type(value = GpsMonthTransfor.class, name = "GMT"),
        @JsonSubTypes.Type(value = GpsYearTransfor.class, name = "GYT"),
        @JsonSubTypes.Type(value = GpsHourTransfor.class, name = "GHT"),
        @JsonSubTypes.Type(value = GpsDayLatestData.class, name = "GDL"),


        @JsonSubTypes.Type(value = AmHourSource.class, name = "AHS"),
        @JsonSubTypes.Type(value = AmHourLatestData.class, name = "AHL"),
        @JsonSubTypes.Type(value = AmDayLatestData.class, name = "ADL"),
        @JsonSubTypes.Type(value = AmDayTransfor.class, name = "ADT"),
        @JsonSubTypes.Type(value = AmMonthTransfor.class, name = "AMT"),
        @JsonSubTypes.Type(value = AmYearTransfor.class, name = "AYT"),
        @JsonSubTypes.Type(value = AmHourTransfor.class, name = "AHT"),

        @JsonSubTypes.Type(value = DeHourSource.class, name = "DHS"),
        @JsonSubTypes.Type(value = DeHourLatestData.class, name = "DHL"),
        @JsonSubTypes.Type(value = DeDayLatestData.class, name = "DDL"),
        @JsonSubTypes.Type(value = DeHourTransfor.class, name = "DHT"),
        @JsonSubTypes.Type(value = DeMonthTransfor.class, name = "DMT"),
        @JsonSubTypes.Type(value = DeYearTransfor.class, name = "DYT"),
        @JsonSubTypes.Type(value = DeDayTransfor.class, name = "DDT"),

        @JsonSubTypes.Type(value = ObdHourSource.class, name = "OHS"),
        @JsonSubTypes.Type(value = ObdHourLatestData.class, name = "OHL"),
        @JsonSubTypes.Type(value = ObdDayLatestData.class, name = "ODL"),
        @JsonSubTypes.Type(value = ObdHourTransfor.class, name = "OHT"),
        @JsonSubTypes.Type(value = ObdMonthTransfor.class, name = "OMT"),
        @JsonSubTypes.Type(value = ObdYearTransfor.class, name = "OYT"),
        @JsonSubTypes.Type(value = ObdDayTransfor.class, name = "ODT"),

        @JsonSubTypes.Type(value = TraceHourSource.class, name = "THS"),
        @JsonSubTypes.Type(value = TraceHourLatestData.class, name = "THL"),
        @JsonSubTypes.Type(value = TraceDayLatestData.class, name = "TDL"),
        @JsonSubTypes.Type(value = TraceHourTransfor.class, name = "THT"),
        @JsonSubTypes.Type(value = TraceMonthTransfor.class, name = "TMT"),
        @JsonSubTypes.Type(value = TraceYearTransfor.class, name = "TYT"),
        @JsonSubTypes.Type(value = TraceDayTransfor.class, name = "TDT"),

        @JsonSubTypes.Type(value = TraceDeleteHourSource.class, name = "TDHS"),
        @JsonSubTypes.Type(value = TraceDeleteHourLatestData.class, name = "TDHL"),
        @JsonSubTypes.Type(value = TraceDeleteDayLatestData.class, name = "TDDL"),
        @JsonSubTypes.Type(value = TraceDeleteHourTransfor.class, name = "TDHT"),
        @JsonSubTypes.Type(value = TraceDeleteMonthTransfor.class, name = "TDMT"),
        @JsonSubTypes.Type(value = TraceDeleteYearTransfor.class, name = "TDYT"),
        @JsonSubTypes.Type(value = TraceDeleteDayTransfor.class, name = "TDDT"),

        @JsonSubTypes.Type(value = VoltageHourSource.class, name = "VHS"),
        @JsonSubTypes.Type(value = VoltageHourLatestData.class, name = "VHL"),
        @JsonSubTypes.Type(value = VoltageDayLatestData.class, name = "VDL"),
        @JsonSubTypes.Type(value = VoltageHourTransfor.class, name = "VHT"),
        @JsonSubTypes.Type(value = VoltageMonthTransfor.class, name = "VMT"),
        @JsonSubTypes.Type(value = VoltageYearTransfor.class, name = "VYT"),
        @JsonSubTypes.Type(value = VoltageDayTransfor.class, name = "VDT"),

        @JsonSubTypes.Type(value = HourIntegratedTransfor.class, name = "HIT"),
        @JsonSubTypes.Type(value = DayIntegratedTransfor.class, name = "DIT"),
        @JsonSubTypes.Type(value = MonthIntegratedTransfor.class, name = "MIT"),
        @JsonSubTypes.Type(value = YearIntegratedTransfor.class, name = "YIT"),

        @JsonSubTypes.Type(value = DormancySource.class, name = "DS"),

        @JsonSubTypes.Type(value = MileageHourSource.class, name = "MHS"),
        @JsonSubTypes.Type(value = MileageHourLatestData.class, name = "MHL"),
        @JsonSubTypes.Type(value = MileageDayLatestData.class, name = "MDL"),
        @JsonSubTypes.Type(value = MileageHourTransfor.class, name = "MHT"),
        @JsonSubTypes.Type(value = MileageMonthTransfor.class, name = "MMT"),
        @JsonSubTypes.Type(value = MileageYearTransfor.class, name = "MYT"),
        @JsonSubTypes.Type(value = MileageDayTransfor.class, name = "MDT"),

})
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ApiModel
@ToString
public class CSTData {
    @ApiModelProperty(value = "车Id")
    protected String carId="";

    @ApiModelProperty(value = "时间戳")
    protected Long time = 0L;

}

package com.cst.stream.stathour.mileage;

import com.cst.stream.stathour.CSTData;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

/**
 * @author Johnney.chiu
 * create on 2017/11/27 14:45
 * @Description 存储于缓存和hbase中的数据
 */
@Getter
@Setter
@ToString(callSuper = true)
@NoArgsConstructor
@ApiModel(value = "MileageMonthTransfor",description = "mileage 月数据结果",parent = CSTData.class)
public class MileageMonthTransfor extends CSTData {


    @ApiModelProperty(value = "gps行程里程")
    private Double milGpsMileage = 0D;

    @ApiModelProperty(value = "obd行程里程")
    private Double milObdMileage = 0D;

    @ApiModelProperty(value = "panel行程里程")
    private Double milPanelMileage = 0D;

    @ApiModelProperty(value = "耗油量")
    private Double milFuel = 0D;

    @ApiModelProperty(value = "行驶时间")
    private Integer milDuration = 0;

    @ApiModelProperty(value = "油费")
    private Double milFee = 0D;

    @ApiModelProperty(value = "obd最大速度")
    private Float milObdMaxSpeed=0f;

    @ApiModelProperty(value = "gps最大速度")
    private Float milGpsMaxSpeed=0f;

    @ApiModelProperty(value = "车gps speed")
    private Integer gpsSpeed=0;

    @ApiModelProperty(value = "车obd speed")
    private Integer obdSpeed=0;

    @ApiModelProperty(value = "上报总里程")
    private Double milGpsTotalDistance=0D;

    @ApiModelProperty(value = "上报总里程")
    private Double milObdTotalDistance=0D;

    @ApiModelProperty(value = "总油耗")
    private Double milTotalFuel=0D;

    @ApiModelProperty(value = "总行驶时间")
    private Long milRunTotalTime=0L;

    @ApiModelProperty(value = "仪表盘车里程")
    private Double panelDistance=0D;


    @Builder
    public MileageMonthTransfor(String carId, Long time, Double milGpsMileage, Double milObdMileage, Double milPanelMileage, Double milFuel, Integer milDuration, Double milFee, Float milObdMaxSpeed, Float milGpsMaxSpeed, Integer gpsSpeed, Integer obdSpeed, Double milGpsTotalDistance, Double milObdTotalDistance, Double milTotalFuel, Long milRunTotalTime, Double panelDistance) {
        super(carId, time);
        this.milGpsMileage = milGpsMileage;
        this.milObdMileage = milObdMileage;
        this.milPanelMileage = milPanelMileage;
        this.milFuel = milFuel;
        this.milDuration = milDuration;
        this.milFee = milFee;
        this.milObdMaxSpeed = milObdMaxSpeed;
        this.milGpsMaxSpeed = milGpsMaxSpeed;
        this.gpsSpeed = gpsSpeed;
        this.obdSpeed = obdSpeed;
        this.milGpsTotalDistance = milGpsTotalDistance;
        this.milObdTotalDistance = milObdTotalDistance;
        this.milTotalFuel = milTotalFuel;
        this.milRunTotalTime = milRunTotalTime;
        this.panelDistance = panelDistance;
    }
}

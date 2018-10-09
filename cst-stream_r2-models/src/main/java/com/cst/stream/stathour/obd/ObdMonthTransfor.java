package com.cst.stream.stathour.obd;

import com.cst.stream.stathour.CSTData;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

/**
 * @author Johnney.chiu
 * create on 2018/05/07 17:30
 * @Description obd 天数据计算
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString(callSuper = true)
@ApiModel(value = "ObdMonthTransfor",description = "obd 月数据",parent = CSTData.class)
public class ObdMonthTransfor extends CSTData {

    @ApiModelProperty(value = "车机号")
    private String din = "";

    @ApiModelProperty(value = "总里程")
    private Float totalDistance = 0f;

    @ApiModelProperty(value = "总油费")
    private Float totalFuel = 0f;

    @ApiModelProperty(value = "最大速度")
    private Float maxSpeed = 0f;

    @ApiModelProperty(value = "高速行驶天数")
    private Integer highSpeedNum = 0;

    @ApiModelProperty(value = "夜间开车天数")
    private Integer nightDriveNum = 0;

    @ApiModelProperty(value = "开车天数")
    private Integer driveNum = 0;

    @ApiModelProperty(value = "行程里程")
    private Float mileage = 0f;

    @ApiModelProperty(value = "耗油量")
    private Float fuel = 0f;

    @ApiModelProperty(value = "行驶时间")
    private Integer duration = 0;

    @ApiModelProperty(value = "油费")
    private Float fee = 0f;

    @ApiModelProperty(value = "速度")
    private Integer speed = 0;

    @ApiModelProperty(value = "运行总时间")
    private Integer runTotalTime = 0;

    @ApiModelProperty(value = "车总里程")
    private Float motormeterDistance = 0f;

    @ApiModelProperty(value = "百公里油耗")
    private Float fuelPerHundred=0f;

    @ApiModelProperty(value = "工装概率")
    private Float toolingProbability=0f;

    @ApiModelProperty(value = "平均速度")
    private Float averageSpeed=0f;

    @ApiModelProperty(value = "车机更改天数")
    private Integer dinChangeNum=0;

    @ApiModelProperty(value = "耗电量")
    private Float powerConsumption=0.0f;

    @Builder
    public ObdMonthTransfor(String carId, Long time, String din, Float totalDistance, Float totalFuel, Float maxSpeed, Integer highSpeedNum, Integer nightDriveNum, Integer driveNum, Float mileage, Float fuel, Integer duration, Float fee, Integer speed, Integer runTotalTime, Float motormeterDistance, Float fuelPerHundred, Float toolingProbability, Float averageSpeed, Integer dinChangeNum, Float powerConsumption) {
        super(carId, time);
        this.din = din;
        this.totalDistance = totalDistance;
        this.totalFuel = totalFuel;
        this.maxSpeed = maxSpeed;
        this.highSpeedNum = highSpeedNum;
        this.nightDriveNum = nightDriveNum;
        this.driveNum = driveNum;
        this.mileage = mileage;
        this.fuel = fuel;
        this.duration = duration;
        this.fee = fee;
        this.speed = speed;
        this.runTotalTime = runTotalTime;
        this.motormeterDistance = motormeterDistance;
        this.fuelPerHundred = fuelPerHundred;
        this.toolingProbability = toolingProbability;
        this.averageSpeed = averageSpeed;
        this.dinChangeNum = dinChangeNum;
        this.powerConsumption = powerConsumption;
    }
}

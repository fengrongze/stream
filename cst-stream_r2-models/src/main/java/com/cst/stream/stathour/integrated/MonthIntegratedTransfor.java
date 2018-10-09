package com.cst.stream.stathour.integrated;

import com.cst.stream.stathour.CSTData;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

/**
 * @author Johnney.Chiu
 * create on 2018/6/29 14:42
 * @Description
 * @title
 */

@Getter
@Setter
@ToString(callSuper = true)
@NoArgsConstructor
@ApiModel(value = "MonthIntegratedTransfor",description = "月数据结果",parent = CSTData.class)
public class MonthIntegratedTransfor extends CSTData{

    @ApiModelProperty(value = "点火次数")
    private Integer ignition=0;

    @ApiModelProperty(value = "熄火次数")
    private Integer flameOut=0;

    @ApiModelProperty(value = "插入次数")
    private Integer insertNum=0;

    @ApiModelProperty(value = "碰撞告警次数")
    private Integer collision=0;

    @ApiModelProperty(value = "超速告警次数")
    private Integer overSpeed=0;

    @ApiModelProperty(value = "失联天数")
    private Integer missingNum=0;

    @ApiModelProperty(value = "拔出时长")
    private Float pulloutTimes=0F;

    @ApiModelProperty(value = "疲劳驾驶天数)")
    private Integer fatigueNum=0;

    @ApiModelProperty(value = "急加速")
    private Integer rapidAccelerationCount=0;

    @ApiModelProperty(value = "急减速")
    private Integer rapidDecelerationCount=0 ;

    @ApiModelProperty(value = "急转弯")
    private Integer sharpTurnCount=0 ;

    @ApiModelProperty(value = "最大搜星数")
    private Integer maxSatelliteNum=0;

    @ApiModelProperty(value = "gps上报数")
    private Integer gpsCount=0;

    @ApiModelProperty(value = "车机号")
    private String din="";

    @ApiModelProperty(value = "车机更改天数")
    private Integer dinChangeNum=0;

    @ApiModelProperty(value = "本地天数")
    private Integer nonLocalNum=0;

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

    @ApiModelProperty(value = "轨迹删除次数")
    private Integer traceDeleteCounts=0;

    @ApiModelProperty(value = "轨迹条数")
    private Integer traceCounts = 0;

    @ApiModelProperty(value = "最大电瓶电压")
    private Float maxVoltage=0f;

    @ApiModelProperty(value = "最小电瓶电压")
    private Float minVoltage=0f;

    @ApiModelProperty(value = "百公里油耗")
    private Float fuelPerHundred=0f;

    @ApiModelProperty(value = "工装概率")
    private Float toolingProbability=0f;

    @ApiModelProperty(value = "平均速度")
    private Float averageSpeed=0f;

    @ApiModelProperty(value = "拔出次数")
    private Integer pulloutCounts=0;

    @ApiModelProperty(value = "耗电量")
    private Float powerConsumption=0.0f;

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
    public MonthIntegratedTransfor(String carId, Long time, Integer ignition, Integer flameOut, Integer insertNum, Integer collision, Integer overSpeed, Integer missingNum, Float pulloutTimes, Integer fatigueNum, Integer rapidAccelerationCount, Integer rapidDecelerationCount, Integer sharpTurnCount, Integer maxSatelliteNum, Integer gpsCount, String din, Integer dinChangeNum, Integer nonLocalNum, Float totalDistance, Float totalFuel, Float maxSpeed, Integer highSpeedNum, Integer nightDriveNum, Integer driveNum, Float mileage, Float fuel, Integer duration, Float fee, Integer speed, Integer runTotalTime, Float motormeterDistance, Integer traceDeleteCounts, Integer traceCounts, Float maxVoltage, Float minVoltage, Float fuelPerHundred, Float toolingProbability, Float averageSpeed, Integer pulloutCounts, Float powerConsumption, Double milGpsMileage, Double milObdMileage, Double milPanelMileage, Double milFuel, Integer milDuration, Double milFee, Float milObdMaxSpeed, Float milGpsMaxSpeed, Integer gpsSpeed, Integer obdSpeed, Double milGpsTotalDistance, Double milObdTotalDistance, Double milTotalFuel, Long milRunTotalTime, Double panelDistance) {
        super(carId, time);
        this.ignition = ignition;
        this.flameOut = flameOut;
        this.insertNum = insertNum;
        this.collision = collision;
        this.overSpeed = overSpeed;
        this.missingNum = missingNum;
        this.pulloutTimes = pulloutTimes;
        this.fatigueNum = fatigueNum;
        this.rapidAccelerationCount = rapidAccelerationCount;
        this.rapidDecelerationCount = rapidDecelerationCount;
        this.sharpTurnCount = sharpTurnCount;
        this.maxSatelliteNum = maxSatelliteNum;
        this.gpsCount = gpsCount;
        this.din = din;
        this.dinChangeNum = dinChangeNum;
        this.nonLocalNum = nonLocalNum;
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
        this.traceDeleteCounts = traceDeleteCounts;
        this.traceCounts = traceCounts;
        this.maxVoltage = maxVoltage;
        this.minVoltage = minVoltage;
        this.fuelPerHundred = fuelPerHundred;
        this.toolingProbability = toolingProbability;
        this.averageSpeed = averageSpeed;
        this.pulloutCounts = pulloutCounts;
        this.powerConsumption = powerConsumption;
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

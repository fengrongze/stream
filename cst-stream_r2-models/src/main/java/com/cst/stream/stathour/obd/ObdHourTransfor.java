package com.cst.stream.stathour.obd;

import com.cst.stream.stathour.CSTData;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;


/**
 * @author Johnney.chiu
 * create on 2017/12/6 11:30
 * @Description obd 的数据处理
 */
@NoArgsConstructor
@Getter
@Setter
@ToString(callSuper = true)
@ApiModel(value = "ObdHourTransfor",description = "obd 小时数据",parent = CSTData.class)

public class ObdHourTransfor extends CSTData {

    @ApiModelProperty(value = "车机号")
    private String din = "";

    @ApiModelProperty(value = "速度")
    private Integer speed = 0;

    @ApiModelProperty(value = "计算总里程")
    private Float totalDistance = 0f;

    @ApiModelProperty(value = "总油耗")
    private Float totalFuel = 0f;

    @ApiModelProperty(value = "运行总时间")
    private Integer runTotalTime = 0;

    @ApiModelProperty(value = "车总里程")
    private Float motormeterDistance = 0f;

    @ApiModelProperty(value = "最大速度")
    private Float maxSpeed = 0f;

    @ApiModelProperty(value = "百公里油耗")
    private Float fuelPerHundred=0f;

    //0未上，1 上高速
    @ApiModelProperty(value = "0未上，1 上高速")
    private Integer isHighSpeed = 0;


    //0表示没有开车，1表示开车
    @ApiModelProperty(value = "0表示没有开车，1表示开车")
    private Integer isDrive = 0;

    //行程里程
    @ApiModelProperty(value = "行程里程")
    private Float mileage = 0f;

    //耗油量
    @ApiModelProperty(value = "耗油量")
    private Float fuel = 0f;

    //行驶时间
    @ApiModelProperty(value = "行驶时间")
    private Integer duration = 0;

    //油费
    @ApiModelProperty(value = "油费")
    private Float fee = 0f;

    @ApiModelProperty(value = "工装概率")
    private Float toolingProbability=0f;

    @ApiModelProperty(value = "平均速度")
    private Float averageSpeed=0f;

    @ApiModelProperty(value = "车机是否更改")
    private Integer dinChange=0;

    @ApiModelProperty(value = "耗电量")
    private Float powerConsumption=0.0f;

    public ObdHourTransfor buildInit(Float maxSpeed, Integer isHighSpeed, Integer isNightDrive, Integer isDrive,
                                     Float mileage, Float fuel, Integer duration, Float fee) {
        this.maxSpeed = maxSpeed;
        this.isHighSpeed = isHighSpeed;
        this.isDrive = isDrive;
        this.mileage = mileage;
        this.fuel = fuel;
        this.duration = duration;
        this.fee = fee;
        return this;
    }

    @Builder
    public ObdHourTransfor(String carId, Long time, String din, Integer speed, Float totalDistance, Float totalFuel, Integer runTotalTime, Float motormeterDistance, Float maxSpeed, Float fuelPerHundred, Integer isHighSpeed, Integer isDrive, Float mileage, Float fuel, Integer duration, Float fee, Float toolingProbability, Float averageSpeed, Integer dinChange, Float powerConsumption) {
        super(carId, time);
        this.din = din;
        this.speed = speed;
        this.totalDistance = totalDistance;
        this.totalFuel = totalFuel;
        this.runTotalTime = runTotalTime;
        this.motormeterDistance = motormeterDistance;
        this.maxSpeed = maxSpeed;
        this.fuelPerHundred = fuelPerHundred;
        this.isHighSpeed = isHighSpeed;
        this.isDrive = isDrive;
        this.mileage = mileage;
        this.fuel = fuel;
        this.duration = duration;
        this.fee = fee;
        this.toolingProbability = toolingProbability;
        this.averageSpeed = averageSpeed;
        this.dinChange = dinChange;
        this.powerConsumption = powerConsumption;
    }
}

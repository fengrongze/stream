package com.cst.stream.stathour.obd;

import com.cst.stream.stathour.CSTData;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

/**
 * @author Johnney.chiu
 * create on 2017/12/6 10:50
 * @Description obd中间计算源
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString(callSuper = true)
@ApiModel(value = "ObdHourLatestData",description = "obd latest data",parent = CSTData.class)
public class ObdHourLatestData extends CSTData{
    @ApiModelProperty(value = "车机")
    private String din="";

    @ApiModelProperty(value = "速度")
    private Integer speed=0;

    @ApiModelProperty(value = "上报总里程")
    private Float totalDistance=0f;

    @ApiModelProperty(value = "总油耗")
    private Float totalFuel=0f;

    @ApiModelProperty(value = "行驶时间")
    private Integer runTotalTime=0;

    @ApiModelProperty(value = "原始车里程")
    private Float motormeterDistance=0f;


    @ApiModelProperty(value = "最大速度")
    private Float maxSpeed = 0f;

    @ApiModelProperty(value = "0未上，1 上高速")
    private Integer isHighSpeed = 0;

    @ApiModelProperty(value = "0表示没有开车，1表示开车")
    private Integer isDrive = 0;

    @ApiModelProperty(value = "车机是否更改")
    private Integer dinChange=0;

    @ApiModelProperty(value = "工装概率")
    private Float toolingProbability=0f;

    @ApiModelProperty(value = "发动机转速")
    private Integer engineSpeed=0;

    @ApiModelProperty(value = "相同发动机转速计数")
    private Integer sameEngineSpeedCount=0;

    @Builder
    public ObdHourLatestData(String carId, Long time, String din, Integer speed, Float totalDistance, Float totalFuel, Integer runTotalTime, Float motormeterDistance, Float maxSpeed, Integer isHighSpeed, Integer isDrive, Integer dinChange, Float toolingProbability, Integer engineSpeed, Integer sameEngineSpeedCount) {
        super(carId, time);
        this.din = din;
        this.speed = speed;
        this.totalDistance = totalDistance;
        this.totalFuel = totalFuel;
        this.runTotalTime = runTotalTime;
        this.motormeterDistance = motormeterDistance;
        this.maxSpeed = maxSpeed;
        this.isHighSpeed = isHighSpeed;
        this.isDrive = isDrive;
        this.dinChange = dinChange;
        this.toolingProbability = toolingProbability;
        this.engineSpeed = engineSpeed;
        this.sameEngineSpeedCount = sameEngineSpeedCount;
    }
}

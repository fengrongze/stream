package com.cst.stream.stathour.obd;

import com.cst.stream.stathour.CSTData;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

/**
 * @author Johnney.chiu
 * create on 2017/12/6 10:50
 * @Description obd数据源
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString(callSuper = true)
@ApiModel(value = "ObdHourSource",description = "obd 源数据",parent = CSTData.class)
public class ObdHourSource extends CSTData{
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

    @ApiModelProperty(value = "发动机转速")
    private Integer engineSpeed=0;

    @ApiModelProperty(value = "电量")
    private Float powerNum=0F;




    public ObdHourSource carId(String carId){
        this.carId = carId;
        return this;
    }

    public ObdHourSource time(Long time){
        this.time = time;
        return this;
    }
    @Builder
    public ObdHourSource(String carId, Long time, String din, Integer speed, Float totalDistance, Float totalFuel, Integer runTotalTime, Float motormeterDistance, Integer engineSpeed, Float powerNum) {
        super(carId, time);
        this.din = din;
        this.speed = speed;
        this.totalDistance = totalDistance;
        this.totalFuel = totalFuel;
        this.runTotalTime = runTotalTime;
        this.motormeterDistance = motormeterDistance;
        this.engineSpeed = engineSpeed;
        this.powerNum = powerNum;
    }
}

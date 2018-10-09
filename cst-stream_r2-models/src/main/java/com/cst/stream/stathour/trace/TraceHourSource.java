package com.cst.stream.stathour.trace;

import com.cst.stream.stathour.CSTData;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

/**
 * @author Johnney.chiu
 * create on 2017/11/28 14:33
 * @Description 轨迹源数据
 */
@Getter
@Setter
@NoArgsConstructor

@ToString(callSuper = true)
@ApiModel(value = "TraceHourSource",description = "trace 源数据",parent = CSTData.class)
public class TraceHourSource  extends CSTData {

    // 轨迹ID
    @ApiModelProperty(value = "轨迹ID")
    private String travelUuid;

    @ApiModelProperty(value = "起点时间")
    private Long startTime;

    @ApiModelProperty(value = "终点时间")
    private Long stopTime;

    // 一个驾驶循环总里程"KM";
    @ApiModelProperty(value = "一个驾驶循环总里程 KM")
    private Float tripDistance;

    //轨迹类型  0：临时轨迹 ； 1：完整轨迹； 2：熄火后轨迹
    @ApiModelProperty(value = "轨迹类型  0：临时轨迹 ； 1：完整轨迹； 2：熄火后轨迹")
    private Integer travelType;

    //轨迹行驶时间
    @ApiModelProperty(value = "轨迹行驶时间")
    private Integer tripDriveTime;


    @Builder
    public TraceHourSource(String carId, Long time, String travelUuid, Long startTime, Long stopTime, Float tripDistance, Integer travelType, Integer tripDriveTime) {
        super(carId, time);
        this.travelUuid = travelUuid;
        this.startTime = startTime;
        this.stopTime = stopTime;
        this.tripDistance = tripDistance;
        this.travelType = travelType;
        this.tripDriveTime = tripDriveTime;
    }
}

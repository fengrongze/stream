package com.cst.stream.stathour.am;


import com.cst.stream.stathour.CSTData;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

/**
 * @author Johnney.chiu
 * create on 2017/11/28 10:00
 * @Description Am 告警数据
 */
@Getter
@Setter
@ToString(callSuper = true)
@NoArgsConstructor
@ApiModel(value = "AmHourTransfor",description = "am 小时计算结果",parent = CSTData.class)
public class AmHourTransfor extends CSTData  {


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

    @ApiModelProperty(value = "是否失联(0表示没有失联，1表示失联)")
    private Integer isMissing=0;

    @ApiModelProperty(value = "拔出时长")
    private Float pulloutTimes=0F;

    @ApiModelProperty(value = "拔出次数")
    private Integer pulloutCounts=0;

    @ApiModelProperty(value = "疲劳驾驶 是否疲劳驾驶(0表示没有疲劳驾驶，1表示有疲劳驾驶室)")
    private Integer isFatigue=0;

    @Builder

    public AmHourTransfor(String carId, Long time, Integer ignition, Integer flameOut, Integer insertNum, Integer collision, Integer overSpeed, Integer isMissing, Float pulloutTimes, Integer pulloutCounts, Integer isFatigue) {
        super(carId, time);
        this.ignition = ignition;
        this.flameOut = flameOut;
        this.insertNum = insertNum;
        this.collision = collision;
        this.overSpeed = overSpeed;
        this.isMissing = isMissing;
        this.pulloutTimes = pulloutTimes;
        this.pulloutCounts = pulloutCounts;
        this.isFatigue = isFatigue;
    }
}

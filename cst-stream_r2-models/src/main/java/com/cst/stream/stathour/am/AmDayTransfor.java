package com.cst.stream.stathour.am;

import com.cst.stream.stathour.CSTData;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

/**
 * @author Johnney.chiu
 * create on 2017/12/5 10:47
 * @Description 天处理
 */


@Setter
@Getter
@ToString(callSuper = true)
@NoArgsConstructor
@ApiModel(value = "AmDayTransfor",description = "am day 天数据",parent = CSTData.class)
public class AmDayTransfor extends CSTData {

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

    @ApiModelProperty(value = "疲劳驾驶 是否疲劳驾驶(0表示没有疲劳驾驶，1表示有疲劳驾驶室)")
    private Integer isFatigue=0;

    @ApiModelProperty(value = "拔出次数")
    private Integer pulloutCounts=0;

    @Builder

    public AmDayTransfor(String carId, Long time, Integer ignition, Integer flameOut, Integer insertNum, Integer collision, Integer overSpeed, Integer isMissing, Float pulloutTimes, Integer isFatigue, Integer pulloutCounts) {
        super(carId, time);
        this.ignition = ignition;
        this.flameOut = flameOut;
        this.insertNum = insertNum;
        this.collision = collision;
        this.overSpeed = overSpeed;
        this.isMissing = isMissing;
        this.pulloutTimes = pulloutTimes;
        this.isFatigue = isFatigue;
        this.pulloutCounts = pulloutCounts;
    }
}

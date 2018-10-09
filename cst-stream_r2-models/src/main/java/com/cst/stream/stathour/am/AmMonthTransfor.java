package com.cst.stream.stathour.am;

import com.cst.stream.stathour.CSTData;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

/**
 * @author Johnney.chiu
 * create on 2018/05/07 10:47
 * @Description 天处理
 */


@Setter
@Getter
@ToString(callSuper = true)
@NoArgsConstructor
@ApiModel(value = "AmMonthTransfor",description = "am 月计算结果",parent = CSTData.class)
public class AmMonthTransfor extends CSTData {

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


    @ApiModelProperty(value = "拔出次数")
    private Integer pulloutCounts=0;

    @Builder
    public AmMonthTransfor(String carId, Long time, Integer ignition, Integer flameOut, Integer insertNum, Integer collision, Integer overSpeed, Integer missingNum, Float pulloutTimes, Integer fatigueNum, Integer pulloutCounts) {
        super(carId, time);
        this.ignition = ignition;
        this.flameOut = flameOut;
        this.insertNum = insertNum;
        this.collision = collision;
        this.overSpeed = overSpeed;
        this.missingNum = missingNum;
        this.pulloutTimes = pulloutTimes;
        this.fatigueNum = fatigueNum;
        this.pulloutCounts = pulloutCounts;
    }
}

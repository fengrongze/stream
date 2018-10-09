package com.cst.stream.stathour.de;

import com.cst.stream.stathour.CSTData;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

/**
 * @author Johnney.chiu
 * create on 2017/11/27 14:55
 * @Description de data source
 */
@Getter
@Setter
@ToString(callSuper = true)
@NoArgsConstructor
@ApiModel(value = "DeDayLatestData",description = "de day latest data ",parent = CSTData.class)
public class DeDayLatestData extends CSTData {


    @ApiModelProperty(value = "速度")
    private Float speed=0f;

    // 驾驶行为类别;
    @ApiModelProperty(value = "驾驶行为类别")
    private Integer actType=0;

    @ApiModelProperty(value = "标记类型")
    private Integer gatherType=0;


    @ApiModelProperty(value = "急加速总次数")
    private Integer rapidAccelerationCounts = 0;

    @ApiModelProperty(value = "急减速总次数")
    private Integer rapidDecelerationCounts = 0;


    @ApiModelProperty(value = "急转弯总次数")
    private Integer sharpTurnCounts=0 ;

    @Builder
    public DeDayLatestData(String carId, Long time,  Float speed, Integer actType, Integer gatherType, Integer rapidAccelerationCounts, Integer rapidDecelerationCounts, Integer sharpTurnCounts) {
        super(carId, time);
        this.speed = speed;
        this.actType = actType;
        this.gatherType = gatherType;
        this.rapidAccelerationCounts = rapidAccelerationCounts;
        this.rapidDecelerationCounts = rapidDecelerationCounts;
        this.sharpTurnCounts = sharpTurnCounts;
    }
}

package com.cst.stream.stathour.de;

import com.cst.stream.stathour.CSTData;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

/**
 * @author Johnney.chiu
 * create on 2018/05/07 15:07
 * @Description De day
 */
@Getter
@Setter
@ToString(callSuper = true)
@NoArgsConstructor
@ApiModel(value = "DeMonthTransfor",description = "de 月数据结果",parent = CSTData.class)
public class DeMonthTransfor extends CSTData {

    //急加速
    @ApiModelProperty(value = "急加速")
    private Integer rapidAccelerationCount=0;
    //急减速
    @ApiModelProperty(value = "急减速")
    private Integer rapidDecelerationCount=0 ;
    //急转弯
    @ApiModelProperty(value = "急转弯")
    private Integer sharpTurnCount=0 ;

    @Builder
    public DeMonthTransfor(String carId, Long time, Integer rapidAccelerationCount, Integer rapidDecelerationCount, Integer sharpTurnCount) {
        this.carId = carId;
        this.time = time;
        this.rapidAccelerationCount = rapidAccelerationCount;
        this.rapidDecelerationCount = rapidDecelerationCount;
        this.sharpTurnCount = sharpTurnCount;
    }



}

package com.cst.stream.stathour.de;

import com.cst.stream.stathour.CSTData;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

/**
 * @author Johnney.chiu
 * create on 2017/12/5 15:07
 * @Description De day
 */
@Getter
@Setter
@ToString(callSuper = true)
@NoArgsConstructor
@ApiModel(value = "DeDayTransfor",description = "de 天计算",parent = CSTData.class)
public class DeDayTransfor extends CSTData {

    //急加速
    @ApiModelProperty(value = "急加速")
    private Integer rapidAccelerationCount=0;
    //急减速
    @ApiModelProperty(value = "急减速")
    private Integer rapidDecelerationCount=0 ;
    //急转弯
    @ApiModelProperty(value = "急转弯")
    private Integer sharpTurnCount=0 ;


    public Long getTime() {
        return time;
    }

    @Builder
    public DeDayTransfor(String carId, Long time, Integer rapidAccelerationCount, Integer rapidDecelerationCount, Integer sharpTurnCount) {
        this.carId = carId;
        this.time = time;
        this.rapidAccelerationCount = rapidAccelerationCount;
        this.rapidDecelerationCount = rapidDecelerationCount;
        this.sharpTurnCount = sharpTurnCount;
    }



}

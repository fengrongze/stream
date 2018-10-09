package com.cst.stream.stathour.de;

import com.cst.stream.stathour.CSTData;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

/**
 * @author Johnney.chiu
 * create on 2017/11/27 16:55
 * @Description driver event data transfor
 */
@Setter
@Getter
@ToString(callSuper = true)
@NoArgsConstructor
@ApiModel(value = "DeHourTransfor",description = "de 小时数据结果",parent = CSTData.class)
public class DeHourTransfor extends CSTData  {

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
    public DeHourTransfor(String carId, Long time, Integer rapidAccelerationCount, Integer rapidDecelerationCount, Integer sharpTurnCount) {
        super(carId,time);
        this.rapidAccelerationCount = rapidAccelerationCount;
        this.rapidDecelerationCount = rapidDecelerationCount;
        this.sharpTurnCount = sharpTurnCount;
    }




}

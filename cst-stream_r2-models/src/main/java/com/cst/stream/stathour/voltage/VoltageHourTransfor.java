package com.cst.stream.stathour.voltage;

import com.cst.stream.stathour.CSTData;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

/**
 * @author Johnney.Chiu
 * create on 2018/4/12 16:05
 * @Description 电瓶电压小时计算
 * @title
 */

@Getter
@Setter
@NoArgsConstructor
@ToString(callSuper = true)
@ApiModel(value = "VoltageHourTransfor",description = "voltage 小时数据",parent = CSTData.class)
public class VoltageHourTransfor extends CSTData{

    @ApiModelProperty(value = "最大电瓶电压")
    private Float maxVoltage=0f;

    @ApiModelProperty(value = "最小电瓶电压")
    private Float minVoltage=0f;
    @Builder
    public VoltageHourTransfor(String carId, Long time, Float maxVoltage, Float minVoltage) {
        super(carId, time);
        this.maxVoltage = maxVoltage;
        this.minVoltage = minVoltage;
    }
}

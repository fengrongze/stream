package com.cst.stream.stathour.voltage;

import com.cst.stream.stathour.CSTData;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

/**
 * @author Johnney.Chiu
 * create on 2018/4/12 15:55
 * @Description 电瓶电压的数据中间计算源
 * @title
 */

@Getter
@Setter
@NoArgsConstructor
@ToString(callSuper = true)
@ApiModel(value = "VoltageHourLatestData",description = "voltage hour latest data",parent = CSTData.class)
public class VoltageHourLatestData extends CSTData {

    @ApiModelProperty(value = "最大电瓶电压")
    private Float maxVoltage=0f;

    @ApiModelProperty(value = "最小电瓶电压")
    private Float minVoltage=0f;

    @Builder
    public VoltageHourLatestData(String carId, Long time, Float maxVoltage, Float minVoltage) {
        super(carId, time);
        this.maxVoltage = maxVoltage;
        this.minVoltage = minVoltage;
    }
}

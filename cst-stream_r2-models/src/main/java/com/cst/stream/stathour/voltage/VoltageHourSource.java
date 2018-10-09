package com.cst.stream.stathour.voltage;

import com.cst.stream.stathour.CSTData;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

/**
 * @author Johnney.Chiu
 * create on 2018/4/12 15:55
 * @Description 电瓶电压的数据来源
 * @title
 */

@Getter
@Setter
@NoArgsConstructor
@ToString(callSuper = true)
@ApiModel(value = "VoltageHourSource",description = "voltage 源数据",parent = CSTData.class)
public class VoltageHourSource extends CSTData {

    @ApiModelProperty(value = "电瓶电压")
    private Float voltage=0f;
    @Builder
    public VoltageHourSource(String carId, Long time, Float voltage) {
        super(carId, time);
        this.voltage = voltage;
    }
}

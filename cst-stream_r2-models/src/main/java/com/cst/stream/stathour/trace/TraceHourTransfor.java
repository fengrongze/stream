package com.cst.stream.stathour.trace;

import com.cst.stream.stathour.CSTData;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

/**
 * @author Johnney.Chiu
 * create on 2018/4/12 15:30
 * @Description 小时计算的数据
 * @title
 */
@Getter
@Setter
@NoArgsConstructor
@ToString(callSuper = true)
@ApiModel(value = "TraceHourTransfor",description = "trace 小时数据",parent = CSTData.class)
public class TraceHourTransfor extends CSTData {

    @ApiModelProperty(value = "轨迹条数")
    private Integer traceCounts = 0;
    @Builder
    public TraceHourTransfor(String carId, Long time, Integer traceCounts) {
        super(carId, time);
        this.traceCounts = traceCounts;
    }
}

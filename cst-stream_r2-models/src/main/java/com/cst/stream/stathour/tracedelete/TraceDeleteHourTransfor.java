package com.cst.stream.stathour.tracedelete;

import com.cst.stream.stathour.CSTData;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

/**
 * @author Johnney.Chiu
 * create on 2018/4/12 18:00
 * @Description 轨迹小时删除统计
 * @title
 */

@Getter
@Setter
@NoArgsConstructor
@ToString(callSuper = true)
@ApiModel(value = "TraceDeleteHourTransfor",description = "trace delete 小时数据",parent = CSTData.class)
public class TraceDeleteHourTransfor extends CSTData{


    @ApiModelProperty(value = "轨迹删除次数")
    private Integer traceDeleteCounts=0;

    @Builder
    public TraceDeleteHourTransfor(String carId, Long time, Integer traceDeleteCounts) {
        super(carId, time);
        this.traceDeleteCounts = traceDeleteCounts;
    }
}

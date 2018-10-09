package com.cst.stream.stathour.tracedelete;

import com.cst.stream.stathour.CSTData;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

/**
 * @author Johnney.Chiu
 * create on 2018/05/07 18:00
 * @Description 轨迹天删除统计
 * @title
 */

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ToString(callSuper = true)
@ApiModel(value = "TraceDeleteYearTransfor",description = "trace delete 年数据",parent = CSTData.class)

public class TraceDeleteYearTransfor extends CSTData{


    @ApiModelProperty(value = "轨迹删除次数")
    private Integer traceDeleteCounts=0;

    @Builder
    public TraceDeleteYearTransfor(String carId, Long time, Integer traceDeleteCounts) {
        super(carId, time);
        this.traceDeleteCounts = traceDeleteCounts;
    }
}

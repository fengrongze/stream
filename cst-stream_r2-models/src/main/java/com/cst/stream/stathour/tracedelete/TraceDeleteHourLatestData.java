package com.cst.stream.stathour.tracedelete;

import com.cst.stream.stathour.CSTData;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

/**
 * @author Johnney.Chiu
 * create on 2018/4/12 16:19
 * @Description 估计删除小时中间计算源
 * @title
 */
@Getter
@Setter
@NoArgsConstructor
@ToString(callSuper = true)
@ApiModel(value = "TraceDeleteHourLatestData",description = "trace delete hour latest data",parent = CSTData.class)
public class TraceDeleteHourLatestData extends CSTData {

    @ApiModelProperty(value = "轨迹删除次数")
    private Integer traceDeleteCounts=0;

    @Builder
    public TraceDeleteHourLatestData(String carId, Long time, Integer traceDeleteCounts) {
        super(carId, time);
        this.traceDeleteCounts = traceDeleteCounts;
    }
}

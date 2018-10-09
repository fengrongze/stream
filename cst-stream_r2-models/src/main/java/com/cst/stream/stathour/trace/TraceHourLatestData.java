package com.cst.stream.stathour.trace;

import com.cst.stream.stathour.CSTData;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Johnney.chiu
 * create on 2017/11/28 14:33
 * @Description 轨迹中间计算源
 */
@Getter
@Setter
@NoArgsConstructor

@ToString(callSuper = true)
@ApiModel(value = "TraceHourLatestData",description = "trace hour latest data",parent = CSTData.class)
public class TraceHourLatestData extends CSTData {

    @ApiModelProperty(value = "有效轨迹")
    private Set<String> traces = new HashSet<>();

    @Builder
    public TraceHourLatestData(String carId, Long time, Set<String> traces) {
        super(carId, time);
        this.traces = traces;
    }
}

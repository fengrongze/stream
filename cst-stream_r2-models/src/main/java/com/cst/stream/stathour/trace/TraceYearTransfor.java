package com.cst.stream.stathour.trace;

import com.cst.stream.stathour.CSTData;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

/**
 * @author Johnney.Chiu
 * create on 2018/05/07 15:53
 * @Description 轨迹transfor
 * @title
 */


@Getter
@Setter
@NoArgsConstructor
@ToString(callSuper = true)
@ApiModel(value = "TraceYearTransfor",description = "trace 年数据",parent = CSTData.class)
public class TraceYearTransfor extends CSTData {

    @ApiModelProperty(value = "轨迹条数")
    private Integer traceCounts = 0;


    @Builder
    public TraceYearTransfor(String carId, Long time, Integer traceCounts) {
        super(carId, time);
        this.traceCounts = traceCounts;
    }
}